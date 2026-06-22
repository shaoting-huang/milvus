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
	"google.golang.org/grpc/credentials/insecure"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

// TestE2EBulkImportMigration is the full runtime migration through the real gRPC catalog
// service: a cluster's RootCoord metadata (in its own etcd) is shipped via BulkImport to the
// service, written into the namespace's TiKV, verified, and c.meta cut over — all without the
// coord ever touching TiKV directly. The backend stays behind the service.
func TestE2EBulkImportMigration(t *testing.T) {
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	ecli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(ep, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer ecli.Close()

	// 1. source: this cluster's own etcd, seeded before migration.
	srcKV := etcdkv.NewEtcdKV(ecli, "by-dev/e2e-mig-src")
	require.NoError(t, srcKV.RemoveWithPrefix(context.Background(), ""))
	source, err := rootcoord.NewMetaTable(context.Background(), kvrootcoord.NewCatalog(srcKV), newMockTSO())
	require.NoError(t, err)
	require.NoError(t, source.CreateDatabase(context.Background(), model.NewDatabase(8800, "wire_db", etcdpb.DatabaseState_DatabaseCreated, nil), 100))

	// 2. real catalog service with per-namespace TiKV (the only thing that touches TiKV).
	nsPrefix := "by-dev/e2e-mig-dst"
	importKV := func(namespace string) kv.MetaKv { return tikvkv.NewTiKV(tikvClient, nsPrefix+"/"+namespace) }
	require.NoError(t, importKV("wireCluster").RemoveWithPrefix(context.Background(), ""))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(srv, NewServer(nil, WithImportKV(importKV)))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := catalogpb.NewCatalogServiceClient(conn)

	// 3. migrate at runtime through the wire.
	sw := rootcoord.NewSwitchableMetaTable(source)
	cfg := rootcoord.MigrationConfig{
		Switchable: sw,
		Source:     source,
		SourceKV:   srcKV,
		Roots:      []string{"root-coord"},
		Namespace:  "wireCluster",
		Client:     client,
		BuildRemote: func() (rootcoord.IMetaTable, error) {
			return rootcoord.NewMetaTable(context.Background(), kvrootcoord.NewCatalog(importKV("wireCluster")), newMockTSO())
		},
	}
	require.NoError(t, rootcoord.Migrate(context.Background(), cfg))

	// 4. after cutover c.meta reads from the TiKV-backed meta and still sees the db.
	got, err := sw.GetDatabaseByName(context.Background(), "wire_db", 0)
	require.NoError(t, err)
	require.Equal(t, int64(8800), got.ID)

	// and the data really landed in the destination TiKV namespace prefix.
	keys, _, err := importKV("wireCluster").LoadWithPrefix(context.Background(), "root-coord")
	require.NoError(t, err)
	require.NotEmpty(t, keys, "migrated metadata must exist in the destination TiKV")
}
