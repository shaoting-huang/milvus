// Command catalogservice runs the RootCoord catalog service as a standalone process: a
// pooled, namespace-aware metadata authority backed by TiKV. Each namespace (cluster-id,
// carried in gRPC metadata) gets its own MetaTable over its own TiKV prefix, so multiple
// milvus clusters share one service while their metadata stays isolated.
//
// HA: the routing Coordinator (etcd) gates the gRPC surface by shard ownership and serves
// the discovery route map, so clients reach each namespace's owner and redirect on failover.
package main

import (
	"context"
	"flag"
	"net"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/catalogservice"
	"github.com/milvus-io/milvus/internal/catalogservice/routing"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:19530", "gRPC listen address (doubles as this node's dial-able id in the route map; use a reachable host, not 0.0.0.0)")
	pd := flag.String("tikv-pd", "127.0.0.1:2389", "TiKV PD endpoint")
	root := flag.String("tikv-root", "by-dev/catalog", "TiKV root prefix; each namespace lands under <root>/<namespace>")
	etcdEndpoints := flag.String("etcd", "127.0.0.1:2379", "etcd endpoints (comma-separated) for the pooled routing control plane")
	routingPrefix := flag.String("routing-prefix", "by-dev/catalog-routing", "etcd key prefix for routing membership + shard ownership")
	flag.Parse()

	paramtable.Init()

	txn, err := txnkv.NewClient([]string{*pd})
	if err != nil {
		log.Fatal("connect TiKV failed", zap.String("pd", *pd), zap.Error(err))
	}

	// Pooled routing control plane on etcd: lease-based membership + shard-ownership CAS. The
	// Coordinator's live loop reconciles this node's shards, gates the gRPC surface by
	// ownership term, and serves the discovery route map. nodeID == listen so the route map
	// hands clients dial-able addresses directly.
	ecli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*etcdEndpoints, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal("connect etcd failed", zap.String("etcd", *etcdEndpoints), zap.Error(err))
	}
	coord := routing.NewCoordinator(ecli, *routingPrefix, *listen, 3, 200*time.Millisecond)
	if err := coord.Start(context.Background()); err != nil {
		log.Fatal("start routing coordinator failed", zap.Error(err))
	}
	defer coord.Close()

	// One monotonic TSO per process is enough for the default-db bootstrap each namespace's
	// MetaTable performs; real DDL timestamps come from the caller.
	var tsoCounter uint64
	mockTSO := &tso.MockAllocator{GenerateTSOF: func(count uint32) (uint64, error) {
		tsoCounter += uint64(count)
		return tsoCounter, nil
	}}

	// One per-namespace TiKV builder, shared by the MetaTable registry and bulk import so a
	// namespace's data and its migrated data land under the same prefix. Only the service
	// touches TiKV — clients reach it solely through gRPC.
	namespaceKV := func(namespace string) kv.MetaKv {
		return tikvkv.NewTiKV(txn, *root+"/"+namespace,
			tikvkv.WithRequestTimeout(paramtable.Get().TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	}
	reg := catalogservice.NewRegistry(func(namespace string) (rootcoord.IMetaTable, error) {
		return rootcoord.NewMetaTable(context.Background(), kvrootcoord.NewCatalog(namespaceKV(namespace)), mockTSO)
	})

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal("listen failed", zap.String("addr", *listen), zap.Error(err))
	}
	srv := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(srv,
		catalogservice.NewServer(
			catalogservice.NewRoutingMetaTable(reg, coord),
			catalogservice.WithImportKV(namespaceKV),
			catalogservice.WithRouteProvider(coord),
			catalogservice.WithRegistry(reg),
		))

	log.Info("catalog service started",
		zap.String("listen", *listen), zap.String("tikvPD", *pd), zap.String("root", *root),
		zap.String("etcd", *etcdEndpoints), zap.String("routingPrefix", *routingPrefix))
	if err := srv.Serve(lis); err != nil {
		log.Fatal("serve failed", zap.Error(err))
	}
}
