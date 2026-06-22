package catalogservice

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestSmokeRunningProcess hits an ALREADY-RUNNING catalog service process (set
// CATALOG_ADDR=host:port) over real gRPC + real TiKV, proving two clusters (namespaces)
// stay isolated end-to-end across a process boundary. Skipped unless CATALOG_ADDR is set.
func TestSmokeRunningProcess(t *testing.T) {
	addr := os.Getenv("CATALOG_ADDR")
	if addr == "" {
		t.Skip("set CATALOG_ADDR=host:port to smoke-test a running catalog service process")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := catalogpb.NewCatalogServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// unique db name per run so reruns against persistent TiKV stay clean
	dbName := "smoke_db_A_" + time.Now().Format("150405.000")
	ctxA := WithNamespace(ctx, "smokeClusterA")
	ctxB := WithNamespace(ctx, "smokeClusterB")

	respA, err := client.CreateDatabase(ctxA, &catalogpb.CreateDatabaseRequest{
		Db: &etcdpb.DatabaseInfo{Id: time.Now().UnixNano() % 1_000_000, Name: dbName, State: etcdpb.DatabaseState_DatabaseCreated},
		Ts: 1,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(respA.GetStatus()))

	// clusterB must not see clusterA's db
	respB, err := client.ListDatabases(ctxB, &catalogpb.ListDatabasesRequest{Ts: 0})
	require.NoError(t, err)
	require.NoError(t, merr.Error(respB.GetStatus()))
	for _, d := range respB.GetDbs() {
		require.NotEqual(t, dbName, d.GetName(), "clusterB saw clusterA's db across the process boundary")
	}

	// clusterA sees its own
	got, err := client.GetDatabaseByName(ctxA, &catalogpb.GetDatabaseByNameRequest{DbName: dbName, Ts: 0})
	require.NoError(t, err)
	require.NoError(t, merr.Error(got.GetStatus()))
	require.Equal(t, dbName, got.GetDb().GetName())

	t.Logf("isolation confirmed against running process %s: %s visible in A, hidden from B", addr, dbName)
}
