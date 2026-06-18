package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- FileResource ----

func (s *Server) AddFileResource(ctx context.Context, req *catalogpb.AddFileResourceRequest) (*catalogpb.AddFileResourceResponse, error) {
	err := s.meta.AddFileResource(ctx, req.GetResource())
	return &catalogpb.AddFileResourceResponse{Status: merr.Status(err)}, nil
}

func (s *Server) RemoveFileResource(ctx context.Context, req *catalogpb.RemoveFileResourceRequest) (*catalogpb.RemoveFileResourceResponse, error) {
	err, removed := s.meta.RemoveFileResource(ctx, req.GetName())
	return &catalogpb.RemoveFileResourceResponse{Status: merr.Status(err), Removed: removed}, nil
}

func (s *Server) ListFileResource(ctx context.Context, req *catalogpb.ListFileResourceRequest) (*catalogpb.ListFileResourceResponse, error) {
	resources, version := s.meta.ListFileResource(ctx)
	return &catalogpb.ListFileResourceResponse{Status: merr.Success(), Resources: resources, Version: version}, nil
}

func (s *Server) IncFileResourceRefCnt(ctx context.Context, req *catalogpb.IncFileResourceRefCntRequest) (*catalogpb.IncFileResourceRefCntResponse, error) {
	err := s.meta.IncFileResourceRefCnt(req.GetIds())
	return &catalogpb.IncFileResourceRefCntResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DecFileResourceRefCnt(ctx context.Context, req *catalogpb.DecFileResourceRefCntRequest) (*catalogpb.DecFileResourceRefCntResponse, error) {
	s.meta.DecFileResourceRefCnt(req.GetIds())
	return &catalogpb.DecFileResourceRefCntResponse{Status: merr.Success()}, nil
}

func (s *Server) RecoverFileResourceRefCnt(ctx context.Context, req *catalogpb.RecoverFileResourceRefCntRequest) (*catalogpb.RecoverFileResourceRefCntResponse, error) {
	pending := make(map[int64][]int64, len(req.GetPendingCollections()))
	for k, v := range req.GetPendingCollections() {
		pending[k] = v.GetIds()
	}
	s.meta.RecoverFileResourceRefCnt(pending)
	return &catalogpb.RecoverFileResourceRefCntResponse{Status: merr.Success()}, nil
}
