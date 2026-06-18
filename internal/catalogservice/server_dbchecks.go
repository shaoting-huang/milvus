package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Database precondition checks (dbchecks group) ----

func (s *Server) CheckIfDatabaseCreatable(ctx context.Context, req *catalogpb.CheckIfDatabaseCreatableRequest) (*catalogpb.CheckIfDatabaseCreatableResponse, error) {
	err := s.meta.CheckIfDatabaseCreatable(ctx, req.GetReq())
	return &catalogpb.CheckIfDatabaseCreatableResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfDatabaseDroppable(ctx context.Context, req *catalogpb.CheckIfDatabaseDroppableRequest) (*catalogpb.CheckIfDatabaseDroppableResponse, error) {
	err := s.meta.CheckIfDatabaseDroppable(ctx, req.GetReq())
	return &catalogpb.CheckIfDatabaseDroppableResponse{Status: merr.Status(err)}, nil
}
