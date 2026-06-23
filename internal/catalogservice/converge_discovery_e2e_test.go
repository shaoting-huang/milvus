package catalogservice

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/catalogservice/client"
	"github.com/milvus-io/milvus/internal/catalogservice/routing"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

// TestE2EConvergeThroughDiscovery is the closest thing to the real RootCoord runtime path
// that the import graph allows: it drives the exact MigrationConfig that Core.catalogConvergeLoop
// builds — but through a real discovery client (client.NewRoutingClient over a route map served
// by a real routing.Coordinator), against a real catalog-service node and a real TiKV/etcd.
//
// This closes the gap between TestE2EBulkImportMigration (which migrates through a DIRECT
// catalogpb client, no discovery) and TestConvergeRoutesWhenMarked (which uses a FAKE client,
// no service). Here ConvergeCatalog runs end to end: gate -> bulk-import over discovery ->
// verify -> cut over to a routing-backed meta -> stamp the marker; then a second call proves
// idempotency (marker present -> route only, no re-migration).
func TestE2EConvergeThroughDiscovery(t *testing.T) {
	ctx := context.Background()
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	ecli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(ep, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer ecli.Close()

	routingPrefix := "catalog-test/converge-disc"
	_, err = ecli.Delete(ctx, routingPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	nsRoot := "by-dev/converge-disc-dst"
	ns := "convCluster"
	require.NoError(t, tikvkv.NewTiKV(tikvClient, nsRoot+"/"+ns).RemoveWithPrefix(ctx, ""))

	// one pooled catalog-service node (coordinator + route provider + bulk-import), exactly as
	// cmd/catalogservice wires it. A single node owns every shard once it converges.
	n := startNode(t, ecli, routingPrefix, nsRoot)
	defer func() { n.srv.Stop(); n.coord.Close() }()
	require.Eventually(t, func() bool {
		return len(n.coord.OwnedShards()) == routing.ShardCount
	}, 12*time.Second, 200*time.Millisecond, "the single node must own all shards before serving")

	// source: this cluster's OWN etcd, seeded with a db before migration.
	srcKV := etcdkv.NewEtcdKV(ecli, "by-dev/converge-disc-src")
	require.NoError(t, srcKV.RemoveWithPrefix(ctx, ""))
	source, err := rootcoord.NewMetaTable(ctx, kvrootcoord.NewCatalog(srcKV), newMockTSO())
	require.NoError(t, err)
	require.NoError(t, source.CreateDatabase(ctx, model.NewDatabase(8810, "disc_db", etcdpb.DatabaseState_DatabaseCreated, nil), 100))

	// the discovery client: every call routes to ShardOf(ns)'s owner via the route map.
	router := client.NewRouter(n.addr)
	defer router.Close()
	rc := client.NewRoutingClient(router, ns)

	sw := rootcoord.NewSwitchableMetaTable(source)
	cfg := rootcoord.MigrationConfig{
		Switchable:  sw,
		Source:      source,
		SourceKV:    srcKV,
		Roots:       []string{"root-coord"},
		Namespace:   ns,
		Client:      rc,
		BuildRemote: func() (rootcoord.IMetaTable, error) { return rootcoord.NewRemoteMetaTable(source, rc), nil },
	}

	// first converge: no marker -> migrate (bulk-import via discovery) -> verify -> cut over.
	require.NoError(t, rootcoord.ConvergeCatalog(ctx, cfg))

	// after cutover, reads go through the discovery client to the service-backed TiKV.
	got, err := sw.GetDatabaseByName(ctx, "disc_db", 0)
	require.NoError(t, err)
	require.Equal(t, int64(8810), got.ID)

	// the durable marker was stamped in the source backend.
	has, err := srcKV.Has(ctx, "root-coord/_catalog_migrated")
	require.NoError(t, err)
	require.True(t, has, "a successful converge must stamp the migrated marker")

	// the data really landed in the namespace's TiKV prefix (only the service touched TiKV).
	keys, _, err := tikvkv.NewTiKV(tikvClient, nsRoot+"/"+ns).LoadWithPrefix(ctx, "root-coord")
	require.NoError(t, err)
	require.NotEmpty(t, keys, "migrated metadata must exist in the destination TiKV namespace")

	// second converge: marker present -> route only, no re-migration; the db is still served.
	require.NoError(t, rootcoord.ConvergeCatalog(ctx, cfg))
	got2, err := sw.GetDatabaseByName(ctx, "disc_db", 0)
	require.NoError(t, err)
	require.Equal(t, int64(8810), got2.ID)
}
