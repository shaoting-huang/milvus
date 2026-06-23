package catalogservice

import (
	"context"
	"strconv"

	"github.com/milvus-io/milvus/internal/catalogservice/nsmeta"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// OwnershipProvider tells the routing meta whether this catalog node may serve a namespace
// and at what ownership term. The routing control plane (routing.Coordinator) satisfies it.
// When nil, the service is single-tenant: the gate is open and the term is 0.
type OwnershipProvider interface {
	IsServable(namespace string) bool
	ShardTerm(namespace string) int64
}

// routingMetaTable is an IMetaTable that dispatches each call to the per-namespace MetaTable
// resolved from the request context. Plugging it into the catalog Server makes the whole
// gRPC surface namespace-aware with no change to the 80 server methods.
//
// resolve() is also the ownership gate: if this node does not currently own the namespace's
// shard (not owner / handing off / stale lease) it returns a retriable error so the client
// re-fetches the route map and redirects to the real owner. The owner's term keys the
// per-namespace cache so a re-claim reloads from the backend before serving.
// It does NOT embed IMetaTable — every one of the 80 methods must route (see the generated
// delegations in routing_meta_gen.go), so a missing method is a compile error, not a runtime
// nil-pointer panic (the bug the real-milvus e2e exposed when ListFileResource hit a nil embed).
//
// KNOWN LIMITATION: four IMetaTable methods take no context.Context
// (GetPartitionIDByName, Inc/Dec/RecoverFileResourceRefCnt). The generated delegations resolve
// them against context.Background(), which always maps to the default namespace — so in a
// multi-tenant deployment these route to "_default" regardless of the caller. This is an
// interface-shape limitation (the server handlers also drop ctx for them); single-tenant is
// fine. Fixing it requires adding a ctx parameter to those IMetaTable methods.
//
//go:generate go run ./cmd/genrouting
type routingMetaTable struct {
	reg      *Registry
	provider OwnershipProvider // nil = single-tenant (open gate, term 0)
}

// Compile-time guarantee that routingMetaTable implements the full IMetaTable surface — if
// the interface gains a method, this breaks the build until `go generate` re-emits the
// delegation in routing_meta_gen.go (rather than failing as a runtime nil-pointer panic).
var _ rootcoord.IMetaTable = (*routingMetaTable)(nil)

// NewRoutingMetaTable wires a namespace-routing IMetaTable over the registry. provider may be
// nil for single-tenant mode.
func NewRoutingMetaTable(reg *Registry, provider OwnershipProvider) rootcoord.IMetaTable {
	return &routingMetaTable{reg: reg, provider: provider}
}

func (r *routingMetaTable) resolve(ctx context.Context) (rootcoord.IMetaTable, error) {
	ns := NamespaceFromContext(ctx)
	var term int64
	if r.provider != nil {
		if !r.provider.IsServable(ns) {
			return nil, merr.WrapErrServiceUnavailable("namespace " + ns + " is not owned by this catalog node")
		}
		term = r.provider.ShardTerm(ns)
		// term fencing: a request that routed against a different ownership term came off a
		// stale route map (ownership moved or was re-claimed). Reject it retriably so the
		// client re-discovers the current owner. The lease window is the primary fence; this
		// closes the gap where a stale route map still points at this node. A zero client term
		// opts out (single-tenant / term-unaware caller).
		if clientTerm := nsmeta.TermFrom(ctx); clientTerm != 0 && clientTerm != term {
			return nil, merr.WrapErrServiceUnavailable("namespace " + ns + " ownership term changed (routed against " +
				strconv.FormatInt(clientTerm, 10) + ", now " + strconv.FormatInt(term, 10) + "); redirect")
		}
	}
	return r.reg.Get(ns, term)
}
