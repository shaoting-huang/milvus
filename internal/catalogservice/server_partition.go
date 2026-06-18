package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Partition DDL ----

func (s *Server) AddPartition(ctx context.Context, req *catalogpb.AddPartitionRequest) (*catalogpb.AddPartitionResponse, error) {
	err := s.meta.AddPartition(ctx, model.UnmarshalPartitionModel(req.GetPartition()))
	return &catalogpb.AddPartitionResponse{Status: merr.Status(err)}, nil
}

func (s *Server) GetPartitionIDByName(ctx context.Context, req *catalogpb.GetPartitionIDByNameRequest) (*catalogpb.GetPartitionIDByNameResponse, error) {
	partitionID, exists := s.meta.GetPartitionIDByName(req.GetCollectionId(), req.GetPartitionName())
	return &catalogpb.GetPartitionIDByNameResponse{
		Status:      merr.Status(nil),
		PartitionId: partitionID,
		Exists:      exists,
	}, nil
}

func (s *Server) DropPartition(ctx context.Context, req *catalogpb.DropPartitionRequest) (*catalogpb.DropPartitionResponse, error) {
	err := s.meta.DropPartition(ctx, req.GetCollectionId(), req.GetPartitionId(), req.GetTs())
	return &catalogpb.DropPartitionResponse{Status: merr.Status(err)}, nil
}

func (s *Server) RemovePartition(ctx context.Context, req *catalogpb.RemovePartitionRequest) (*catalogpb.RemovePartitionResponse, error) {
	err := s.meta.RemovePartition(ctx, req.GetCollectionId(), req.GetPartitionId(), req.GetTs())
	return &catalogpb.RemovePartitionResponse{Status: merr.Status(err)}, nil
}
