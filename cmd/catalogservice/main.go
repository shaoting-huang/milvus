// Command catalogservice runs the RootCoord catalog service as a standalone process: a
// pooled, namespace-aware metadata authority backed by TiKV. Each namespace (cluster-id,
// carried in gRPC metadata) gets its own MetaTable over its own TiKV prefix, so multiple
// milvus clusters share one service while their metadata stays isolated.
//
// PoC entrypoint — wiring only, no HA/routing membership yet.
package main

import (
	"context"
	"flag"
	"net"
	"time"

	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/catalogservice"
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
	listen := flag.String("listen", "127.0.0.1:19530", "gRPC listen address")
	pd := flag.String("tikv-pd", "127.0.0.1:2389", "TiKV PD endpoint")
	root := flag.String("tikv-root", "by-dev/catalog", "TiKV root prefix; each namespace lands under <root>/<namespace>")
	flag.Parse()

	paramtable.Init()

	txn, err := txnkv.NewClient([]string{*pd})
	if err != nil {
		log.Fatal("connect TiKV failed", zap.String("pd", *pd), zap.Error(err))
	}

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
		catalogservice.NewServer(catalogservice.NewRoutingMetaTable(reg, nil), catalogservice.WithImportKV(namespaceKV)))

	log.Info("catalog service started", zap.String("listen", *listen), zap.String("tikvPD", *pd), zap.String("root", *root))
	if err := srv.Serve(lis); err != nil {
		log.Fatal("serve failed", zap.Error(err))
	}
}
