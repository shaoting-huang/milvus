// Package catalogservice is the RootCoord catalog service.
//
// It exposes RootCoord's IMetaTable surface over gRPC: the real *MetaTable (ddLock +
// permissionLock + in-memory db/collection/name/alias maps + RootCoordCatalog
// persistence on TiKV) lives inside this server process and serializes every remote
// caller. The coord side runs a thin client (rootcoord.remoteMetaTable) with zero
// metadata cache.
//
// The boundary is the SEMANTIC meta layer (IMetaTable methods), not the KV layer. For
// the DDL methods whose meta-apply is driven by a streaming BroadcastResult, only the
// apply moves here — the broadcast itself stays in RootCoord; this server merely rebuilds
// the BroadcastResult value (in memory, never transmitting) to call the unchanged meta.
package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Server implements catalogpb.CatalogServiceServer by driving the real rootcoord
// MetaTable. Concurrent client RPCs are serialized by the real ddLock/permissionLock
// inside the MetaTable methods, which persist through RootCoordCatalog before mutating
// the in-memory maps. Errors are returned via common.Status (merr.Status).
type Server struct {
	catalogpb.UnimplementedCatalogServiceServer
	meta rootcoord.IMetaTable
}

// NewServer wires the service over an already-constructed real MetaTable.
func NewServer(meta rootcoord.IMetaTable) *Server {
	return &Server{meta: meta}
}

// ---- Database DDL ----

func (s *Server) CreateDatabase(ctx context.Context, req *catalogpb.CreateDatabaseRequest) (*catalogpb.CreateDatabaseResponse, error) {
	err := s.meta.CreateDatabase(ctx, model.UnmarshalDatabaseModel(req.GetDb()), req.GetTs())
	return &catalogpb.CreateDatabaseResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DropDatabase(ctx context.Context, req *catalogpb.DropDatabaseRequest) (*catalogpb.DropDatabaseResponse, error) {
	err := s.meta.DropDatabase(ctx, req.GetDbName(), req.GetTs())
	return &catalogpb.DropDatabaseResponse{Status: merr.Status(err)}, nil
}

func (s *Server) AlterDatabase(ctx context.Context, req *catalogpb.AlterDatabaseRequest) (*catalogpb.AlterDatabaseResponse, error) {
	err := s.meta.AlterDatabase(ctx, model.UnmarshalDatabaseModel(req.GetDb()), req.GetTs())
	return &catalogpb.AlterDatabaseResponse{Status: merr.Status(err)}, nil
}

func (s *Server) ListDatabases(ctx context.Context, req *catalogpb.ListDatabasesRequest) (*catalogpb.ListDatabasesResponse, error) {
	dbs, err := s.meta.ListDatabases(ctx, req.GetTs())
	resp := &catalogpb.ListDatabasesResponse{Status: merr.Status(err)}
	if err == nil {
		for _, db := range dbs {
			resp.Dbs = append(resp.Dbs, model.MarshalDatabaseModel(db))
		}
	}
	return resp, nil
}

func (s *Server) GetDatabaseByName(ctx context.Context, req *catalogpb.GetDatabaseByNameRequest) (*catalogpb.GetDatabaseResponse, error) {
	db, err := s.meta.GetDatabaseByName(ctx, req.GetDbName(), req.GetTs())
	resp := &catalogpb.GetDatabaseResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Db = model.MarshalDatabaseModel(db)
	}
	return resp, nil
}

func (s *Server) GetDatabaseByID(ctx context.Context, req *catalogpb.GetDatabaseByIDRequest) (*catalogpb.GetDatabaseResponse, error) {
	db, err := s.meta.GetDatabaseByID(ctx, req.GetDbId(), req.GetTs())
	resp := &catalogpb.GetDatabaseResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Db = model.MarshalDatabaseModel(db)
	}
	return resp, nil
}
