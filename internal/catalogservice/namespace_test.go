package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// Namespace travels in gRPC metadata: the coord-side client stamps its cluster-id on the
// outgoing context; the service reads it off the incoming context and routes to that
// namespace's MetaTable.

func TestNamespaceFromContextDefault(t *testing.T) {
	// no metadata -> default namespace (single-tenant / not stamped)
	require.Equal(t, DefaultNamespace, NamespaceFromContext(context.Background()))
}

func TestNamespaceFromIncomingMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(namespaceMetadataKey, "clusterX"))
	require.Equal(t, "clusterX", NamespaceFromContext(ctx))
}

func TestWithNamespaceStampsOutgoing(t *testing.T) {
	ctx := WithNamespace(context.Background(), "clusterY")
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{"clusterY"}, md.Get(namespaceMetadataKey))
}

// RoutingMetaTable dispatches each call to the namespace's MetaTable resolved from the
// request context. Two namespaces stay isolated even through the same routing meta.
func TestRoutingMetaTableIsolation(t *testing.T) {
	reg := newTestRegistry()
	rt := NewRoutingMetaTable(reg, nil)

	ctxA := metadata.NewIncomingContext(context.Background(), metadata.Pairs(namespaceMetadataKey, "rtClusterA"))
	ctxB := metadata.NewIncomingContext(context.Background(), metadata.Pairs(namespaceMetadataKey, "rtClusterB"))

	require.NoError(t, rt.CreateDatabase(ctxA, dbModel(9100, "db_rt_A"), 1))

	dbsB, err := rt.ListDatabases(ctxB, 0)
	require.NoError(t, err)
	for _, d := range dbsB {
		require.NotEqual(t, "db_rt_A", d.Name, "namespace B saw namespace A's db through routing meta")
	}

	got, err := rt.GetDatabaseByName(ctxA, "db_rt_A", 0)
	require.NoError(t, err)
	require.Equal(t, int64(9100), got.ID)
}
