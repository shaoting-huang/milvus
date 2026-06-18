package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// remoteMetaTable is the coord-side thin client for the catalog service. It implements
// IMetaTable but routes the migrated methods to the catalog service over gRPC and holds
// ZERO metadata cache locally — the real ddLock + maps + RootCoordCatalog persistence
// live in the service process. Error codes propagate via common.Status (merr.Error).
//
// Methods not yet migrated fall through to the embedded fallback. In the end-state every
// method is remote and the fallback is removed.
type remoteMetaTable struct {
	IMetaTable // fallback for not-yet-migrated methods (nil once everything is remote)
	client     catalogpb.CatalogServiceClient
}

// compile-time proof that the thin client satisfies the full IMetaTable surface.
var _ IMetaTable = (*remoteMetaTable)(nil)

// NewRemoteMetaTable wires a coord-side IMetaTable whose migrated methods are served by
// the catalog service. fallback serves the not-yet-migrated methods.
func NewRemoteMetaTable(fallback IMetaTable, client catalogpb.CatalogServiceClient) IMetaTable {
	return &remoteMetaTable{IMetaTable: fallback, client: client}
}

// ---- Database DDL ----

func (r *remoteMetaTable) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	resp, err := r.client.CreateDatabase(ctx, &catalogpb.CreateDatabaseRequest{Db: model.MarshalDatabaseModel(db), Ts: ts})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error {
	resp, err := r.client.DropDatabase(ctx, &catalogpb.DropDatabaseRequest{DbName: dbName, Ts: ts})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	resp, err := r.client.AlterDatabase(ctx, &catalogpb.AlterDatabaseRequest{Db: model.MarshalDatabaseModel(newDB), Ts: ts})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	resp, err := r.client.ListDatabases(ctx, &catalogpb.ListDatabasesRequest{Ts: ts})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	out := make([]*model.Database, 0, len(resp.GetDbs()))
	for _, info := range resp.GetDbs() {
		out = append(out, model.UnmarshalDatabaseModel(info))
	}
	return out, nil
}

func (r *remoteMetaTable) GetDatabaseByName(ctx context.Context, dbName string, ts typeutil.Timestamp) (*model.Database, error) {
	resp, err := r.client.GetDatabaseByName(ctx, &catalogpb.GetDatabaseByNameRequest{DbName: dbName, Ts: ts})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return model.UnmarshalDatabaseModel(resp.GetDb()), nil
}

func (r *remoteMetaTable) GetDatabaseByID(ctx context.Context, dbID int64, ts typeutil.Timestamp) (*model.Database, error) {
	resp, err := r.client.GetDatabaseByID(ctx, &catalogpb.GetDatabaseByIDRequest{DbId: dbID, Ts: ts})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return model.UnmarshalDatabaseModel(resp.GetDb()), nil
}
