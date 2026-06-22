package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/rootcoord"
)

// newTestRegistry builds a namespace Registry whose per-namespace MetaTable lives over a
// distinct TiKV prefix "by-dev/ns-iso/<namespace>" — this prefix isolation is exactly how
// a pooled catalog service keeps two milvus clusters' metadata apart on one TiKV backend.
func newTestRegistry() *Registry {
	return NewRegistry(func(namespace string) (rootcoord.IMetaTable, error) {
		mkv := tikvkv.NewTiKV(tikvClient, "by-dev/ns-iso/"+namespace)
		return rootcoord.NewMetaTable(context.Background(), kvrootcoord.NewCatalog(mkv), newMockTSO())
	})
}

// TestNamespaceRegistryIsolation is the data-isolation guarantee: two clusters (namespaces)
// served by ONE pooled catalog service cannot see each other's metadata.
func TestNamespaceRegistryIsolation(t *testing.T) {
	reg := newTestRegistry()
	ctx := context.Background()

	a, err := reg.Get("clusterA", 0)
	require.NoError(t, err)
	b, err := reg.Get("clusterB", 0)
	require.NoError(t, err)

	require.NoError(t, a.CreateDatabase(ctx, dbModel(9001, "db_only_in_A"), 1))

	// clusterB must NOT see clusterA's database.
	dbsB, err := b.ListDatabases(ctx, 0)
	require.NoError(t, err)
	for _, d := range dbsB {
		require.NotEqual(t, "db_only_in_A", d.Name, "clusterB leaked clusterA's database")
	}

	// clusterA sees its own.
	got, err := a.GetDatabaseByName(ctx, "db_only_in_A", 0)
	require.NoError(t, err)
	require.Equal(t, int64(9001), got.ID)
}

// TestNamespaceRegistryCaches: the same namespace resolves to the same MetaTable instance,
// so its ddLock + cache are shared across requests (one authority per cluster).
func TestNamespaceRegistryCaches(t *testing.T) {
	reg := newTestRegistry()
	a1, err := reg.Get("clusterCache", 0)
	require.NoError(t, err)
	a2, err := reg.Get("clusterCache", 0)
	require.NoError(t, err)
	require.Same(t, a1, a2, "same namespace must reuse the same MetaTable")
}
