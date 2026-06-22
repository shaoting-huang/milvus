package catalogservice

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/catalogservice/client"
	"github.com/milvus-io/milvus/internal/catalogservice/routing"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type node struct {
	addr  string
	coord *routing.Coordinator
	srv   *grpc.Server
}

func (n *node) kill() {
	n.srv.Stop()    // address unreachable -> client sees gRPC Unavailable
	n.coord.Close() // revoke lease -> the other node claims the freed shards promptly
}

// startNode brings up one pooled catalog-service node: its routing Coordinator (etcd) plus a
// gRPC Server gated by that coordinator (ownership gate + route map). nodeID == listen addr,
// so the route map yields dial-able addresses directly.
func startNode(t *testing.T, ecli *clientv3.Client, prefix, nsRoot string) *node {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	coord := routing.NewCoordinator(ecli, prefix, addr, 3, 200*time.Millisecond)
	require.NoError(t, coord.Start(context.Background()))

	reg := NewRegistry(func(namespace string) (rootcoord.IMetaTable, error) {
		return rootcoord.NewMetaTable(context.Background(),
			kvrootcoord.NewCatalog(tikvkv.NewTiKV(tikvClient, nsRoot+"/"+namespace)), newMockTSO())
	})
	srv := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(srv, NewServer(
		NewRoutingMetaTable(reg, coord),
		WithRouteProvider(coord),
		WithRegistry(reg),
	))
	go func() { _ = srv.Serve(lis) }()
	return &node{addr: addr, coord: coord, srv: srv}
}

// TestTwoNodeFailoverRedirect is the RootCoord end-to-end proof: a client discovers the owner
// of a namespace, reads/writes through it, and when that owner is killed the client transparently
// re-discovers and redirects to the new owner — which reloaded the metadata from TiKV. No data loss.
func TestTwoNodeFailoverRedirect(t *testing.T) {
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	ecli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(ep, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer ecli.Close()
	prefix := "catalog-test/2node-e2e"
	_, err = ecli.Delete(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	nsRoot := "by-dev/2node-e2e"

	n1 := startNode(t, ecli, prefix, nsRoot)
	n2 := startNode(t, ecli, prefix, nsRoot)
	defer func() { n1.srv.Stop(); n1.coord.Close(); n2.srv.Stop(); n2.coord.Close() }()

	// both nodes converge to a disjoint cover of all shards.
	require.Eventually(t, func() bool {
		return len(n1.coord.OwnedShards())+len(n2.coord.OwnedShards()) == routing.ShardCount &&
			len(n1.coord.OwnedShards()) > 0 && len(n2.coord.OwnedShards()) > 0
	}, 12*time.Second, 200*time.Millisecond)

	router := client.NewRouter(n1.addr, n2.addr)
	defer router.Close()
	ctx := context.Background()
	ns := "wireCluster"

	// write + read through discovery (routed to whoever owns ShardOf(ns)).
	require.NoError(t, router.Do(ctx, ns, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		resp, err := cli.CreateDatabase(c, &catalogpb.CreateDatabaseRequest{
			Db: &etcdpb.DatabaseInfo{Id: 8888, Name: "e2e_db", State: etcdpb.DatabaseState_DatabaseCreated}, Ts: 1,
		})
		if err != nil {
			return err
		}
		return merr.Error(resp.GetStatus())
	}))

	// kill the current owner of this namespace.
	require.NoError(t, router.Refresh(ctx))
	owner := router.OwnerOf(ns)
	require.Contains(t, []string{n1.addr, n2.addr}, owner)
	if owner == n1.addr {
		n1.kill()
	} else {
		n2.kill()
	}

	// the client transparently redirects to the new owner, which reloaded from TiKV: the db
	// written before failover is still visible.
	require.Eventually(t, func() bool {
		var gotID int64
		err := router.Do(ctx, ns, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
			resp, err := cli.GetDatabaseByName(c, &catalogpb.GetDatabaseByNameRequest{DbName: "e2e_db", Ts: 0})
			if err != nil {
				return err
			}
			if err := merr.Error(resp.GetStatus()); err != nil {
				return err
			}
			gotID = resp.GetDb().GetId()
			return nil
		})
		return err == nil && gotID == 8888
	}, 12*time.Second, 300*time.Millisecond, "client must redirect to the new owner and still read the db after failover")
}
