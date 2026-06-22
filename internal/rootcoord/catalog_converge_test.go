package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

// convergeCatalog is what the enabled-flag watcher runs to reach the desired state ("on the
// catalog service"): if not yet migrated it migrates then cuts over and stamps a durable
// marker; if the marker is present it just routes (no re-migration). Idempotent across
// restarts via the marker.

func TestConvergeMigratesWhenNoMarker(t *testing.T) {
	source, srcKV := newSourceMeta(t, "by-dev/conv-fresh-src")
	require.NoError(t, source.CreateDatabase(context.Background(), model.NewDatabase(7100, "conv_db", etcdpb.DatabaseState_DatabaseCreated, nil), 100))
	sw := NewSwitchableMetaTable(source)
	fake := &fakeBulkClient{}

	routerBacked := source // sentinel cutover target
	cfg := MigrationConfig{
		Switchable: sw, Source: source, SourceKV: srcKV,
		Roots: []string{"root-coord"}, Namespace: "convCluster", Client: fake,
		BuildRemote: func() (IMetaTable, error) { return routerBacked, nil },
	}
	require.NoError(t, convergeCatalog(context.Background(), cfg))

	require.NotEmpty(t, fake.imported, "fresh cluster must be migrated (bulk-imported)")
	has, err := srcKV.Has(context.Background(), catalogMigratedMarker)
	require.NoError(t, err)
	require.True(t, has, "a successful migration must stamp the durable marker")
}

func TestConvergeRoutesWhenMarked(t *testing.T) {
	source, srcKV := newSourceMeta(t, "by-dev/conv-marked-src")
	require.NoError(t, srcKV.Save(context.Background(), catalogMigratedMarker, "1")) // already migrated
	sw := NewSwitchableMetaTable(source)
	fake := &fakeBulkClient{}

	var built bool
	cfg := MigrationConfig{
		Switchable: sw, Source: source, SourceKV: srcKV,
		Roots: []string{"root-coord"}, Namespace: "convCluster", Client: fake,
		BuildRemote: func() (IMetaTable, error) { built = true; return source, nil },
	}
	require.NoError(t, convergeCatalog(context.Background(), cfg))

	require.Empty(t, fake.imported, "an already-migrated cluster must NOT re-import")
	require.True(t, built, "an already-migrated cluster must still cut over (route to the service)")
}
