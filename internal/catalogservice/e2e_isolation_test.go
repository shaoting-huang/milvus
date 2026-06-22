package catalogservice

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestE2EPooledServiceIsolation is the end-to-end data-isolation proof through the real
// gRPC stack and the real TiKV backend: one pooled catalog service process, two clusters
// (namespaces) stamped on the gRPC metadata, neither can see the other's metadata.
func TestE2EPooledServiceIsolation(t *testing.T) {
	// One pooled service: routing meta over a registry that gives each namespace its own
	// TiKV prefix. This is the same wiring the cmd entrypoint uses.
	reg := NewRegistry(func(namespace string) (rootcoord.IMetaTable, error) {
		mkv := tikvkv.NewTiKV(tikvClient, "by-dev/e2e-iso/"+namespace)
		return rootcoord.NewMetaTable(context.Background(), kvrootcoord.NewCatalog(mkv), newMockTSO())
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(srv, NewServer(NewRoutingMetaTable(reg, nil)))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := catalogpb.NewCatalogServiceClient(conn)

	ctxA := WithNamespace(context.Background(), "e2eClusterA")
	ctxB := WithNamespace(context.Background(), "e2eClusterB")

	// clusterA creates a database through the real gRPC service.
	respA, err := client.CreateDatabase(ctxA, &catalogpb.CreateDatabaseRequest{
		Db: &etcdpb.DatabaseInfo{Id: 9200, Name: "db_e2e_A", State: etcdpb.DatabaseState_DatabaseCreated},
		Ts: 1,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(respA.GetStatus()))

	// clusterB lists databases — must NOT see clusterA's db.
	respB, err := client.ListDatabases(ctxB, &catalogpb.ListDatabasesRequest{Ts: 0})
	require.NoError(t, err)
	require.NoError(t, merr.Error(respB.GetStatus()))
	for _, d := range respB.GetDbs() {
		require.NotEqual(t, "db_e2e_A", d.GetName(), "clusterB saw clusterA's db over gRPC")
	}

	// clusterA sees its own db.
	got, err := client.GetDatabaseByName(ctxA, &catalogpb.GetDatabaseByNameRequest{DbName: "db_e2e_A", Ts: 0})
	require.NoError(t, err)
	require.NoError(t, merr.Error(got.GetStatus()))
	require.Equal(t, int64(9200), got.GetDb().GetId())
}
