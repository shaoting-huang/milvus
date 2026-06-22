package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
type routingMetaTable struct {
	rootcoord.IMetaTable
	reg      *Registry
	provider OwnershipProvider // nil = single-tenant (open gate, term 0)
}

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
	}
	return r.reg.Get(ns, term)
}

func (r *routingMetaTable) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	m, err := r.resolve(ctx)
	if err != nil {
		return err
	}
	return m.CreateDatabase(ctx, db, ts)
}

func (r *routingMetaTable) DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error {
	m, err := r.resolve(ctx)
	if err != nil {
		return err
	}
	return m.DropDatabase(ctx, dbName, ts)
}

func (r *routingMetaTable) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	m, err := r.resolve(ctx)
	if err != nil {
		return err
	}
	return m.AlterDatabase(ctx, newDB, ts)
}

func (r *routingMetaTable) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	m, err := r.resolve(ctx)
	if err != nil {
		return nil, err
	}
	return m.ListDatabases(ctx, ts)
}

func (r *routingMetaTable) GetDatabaseByName(ctx context.Context, dbName string, ts typeutil.Timestamp) (*model.Database, error) {
	m, err := r.resolve(ctx)
	if err != nil {
		return nil, err
	}
	return m.GetDatabaseByName(ctx, dbName, ts)
}

func (r *routingMetaTable) GetDatabaseByID(ctx context.Context, dbID int64, ts typeutil.Timestamp) (*model.Database, error) {
	m, err := r.resolve(ctx)
	if err != nil {
		return nil, err
	}
	return m.GetDatabaseByID(ctx, dbID, ts)
}
