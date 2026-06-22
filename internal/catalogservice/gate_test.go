package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeProvider struct {
	servable bool
	term     int64
}

func (f fakeProvider) IsServable(string) bool { return f.servable }
func (f fakeProvider) ShardTerm(string) int64 { return f.term }

func incomingCtx(ns string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(namespaceMetadataKey, ns))
}

// TestGateRejectsNonOwner: when this node does not own the namespace, the routing meta
// returns a retriable error so the client re-fetches the route map and redirects.
func TestGateRejectsNonOwner(t *testing.T) {
	rt := NewRoutingMetaTable(newTestRegistry(), fakeProvider{servable: false})
	err := rt.CreateDatabase(incomingCtx("ns_not_mine"), dbModel(9400, "db_x"), 1)
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err), "not-owner must be retriable so the client redirects")
}

// TestGateServesOwner: when this node owns the namespace, the call is delegated to its meta.
func TestGateServesOwner(t *testing.T) {
	rt := NewRoutingMetaTable(newTestRegistry(), fakeProvider{servable: true, term: 1})
	ctx := incomingCtx("ns_gate_owner")
	require.NoError(t, rt.CreateDatabase(ctx, dbModel(9500, "db_gate"), 1))
	got, err := rt.GetDatabaseByName(ctx, "db_gate", 0)
	require.NoError(t, err)
	require.Equal(t, int64(9500), got.ID)
}

// TestRegistryReloadsOnTermChange: a term bump (ownership re-claim) rebuilds the namespace's
// MetaTable so a re-acquired shard reloads from the backend instead of serving a stale view.
func TestRegistryReloadsOnTermChange(t *testing.T) {
	reg := newTestRegistry()
	m1, err := reg.Get("ns_term", 1)
	require.NoError(t, err)
	m1b, err := reg.Get("ns_term", 1)
	require.NoError(t, err)
	require.Same(t, m1, m1b, "same term reuses the cached MetaTable")

	m2, err := reg.Get("ns_term", 2)
	require.NoError(t, err)
	require.NotSame(t, m1, m2, "a higher term must rebuild (reload) the MetaTable")
}
