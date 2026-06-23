package catalogservice

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/catalogservice/nsmeta"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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

func incomingCtxWithTerm(ns string, term int64) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(namespaceMetadataKey, ns, nsmeta.TermKey, strconv.FormatInt(term, 10)))
}

// TestGateRejectsStaleTerm: a request that routed against an older ownership term (off a stale
// route map) must be rejected retriably even though this node currently owns the shard — the
// client should re-discover and redirect to the authoritative owner/term.
func TestGateRejectsStaleTerm(t *testing.T) {
	rt := NewRoutingMetaTable(newTestRegistry(), fakeProvider{servable: true, term: 5})
	err := rt.CreateDatabase(incomingCtxWithTerm("ns_stale", 3), dbModel(9600, "db_stale"), 1)
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err), "a stale ownership term must be retriable so the client redirects")
}

// TestGateServesMatchingTerm: a request whose routed term matches the current ownership term
// is served normally.
func TestGateServesMatchingTerm(t *testing.T) {
	rt := NewRoutingMetaTable(newTestRegistry(), fakeProvider{servable: true, term: 5})
	ctx := incomingCtxWithTerm("ns_match", 5)
	require.NoError(t, rt.CreateDatabase(ctx, dbModel(9601, "db_match"), 1))
	got, err := rt.GetDatabaseByName(ctx, "db_match", 0)
	require.NoError(t, err)
	require.Equal(t, int64(9601), got.ID)
}

// TestGateServesZeroClientTerm: a term-unaware caller (no term stamped, term 0) opts out of
// fencing — back-compat with single-tenant clients and the existing route map.
func TestGateServesZeroClientTerm(t *testing.T) {
	rt := NewRoutingMetaTable(newTestRegistry(), fakeProvider{servable: true, term: 5})
	require.NoError(t, rt.CreateDatabase(incomingCtx("ns_zero"), dbModel(9602, "db_zero"), 1))
}

// TestRoutingNonDatabaseMethod guards the nil-embed regression that the real-milvus e2e
// exposed: routingMetaTable must route EVERY IMetaTable method, not just the Database ones.
// Before routing_meta_gen.go, non-Database methods (e.g. ListFileResource, which a real
// rootcoord calls on reload) fell through to a nil embedded interface and panicked the whole
// catalog process. This exercises a non-Database read+write through the routing layer; the
// point is that it does NOT panic and actually delegates to the per-namespace MetaTable.
func TestRoutingNonDatabaseMethod(t *testing.T) {
	rt := NewRoutingMetaTable(newTestRegistry(), fakeProvider{servable: true, term: 1})
	ctx := incomingCtx("ns_nondb")

	// read path (the exact method that nil-panicked in the e2e) — must return, not crash.
	resources, _ := rt.ListFileResource(ctx)
	require.Empty(t, resources, "fresh namespace has no file resources (and crucially: no panic)")

	// write path through routing, then read it back through routing.
	require.NoError(t, rt.AddFileResource(ctx, &internalpb.FileResourceInfo{Name: "fr_route", Id: 7777, Path: "/p"}))
	resources, _ = rt.ListFileResource(ctx)
	ids := make([]int64, 0, len(resources))
	for _, r := range resources {
		ids = append(ids, r.GetId())
	}
	require.Contains(t, ids, int64(7777), "non-Database write must route to the namespace's MetaTable")
}
