package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/catalogservice/migration"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Migration: bulk import + verify ----
//
// The coord reads its own source (etcd) and ships the key/value snapshot here; the service
// writes it into the namespace's TiKV prefix and verifies it. The backend never crosses the
// gRPC boundary — only logical KVs and a mismatch report do.

func (s *Server) BulkImport(ctx context.Context, req *catalogpb.BulkImportRequest) (*catalogpb.BulkImportResponse, error) {
	if s.importKV == nil {
		return &catalogpb.BulkImportResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg("bulk import not enabled"))}, nil
	}
	dst := s.importKV(req.GetNamespace())
	kvs := make(map[string]string, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		kvs[e.GetKey()] = string(e.GetValue())
	}
	if len(kvs) > 0 {
		if err := dst.MultiSave(ctx, kvs); err != nil {
			return &catalogpb.BulkImportResponse{Status: merr.Status(err)}, nil
		}
	}
	return &catalogpb.BulkImportResponse{Status: merr.Success(), Imported: int64(len(kvs))}, nil
}

func (s *Server) VerifyImport(ctx context.Context, req *catalogpb.VerifyImportRequest) (*catalogpb.VerifyImportResponse, error) {
	if s.importKV == nil {
		return &catalogpb.VerifyImportResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg("bulk import not enabled"))}, nil
	}
	dst := s.importKV(req.GetNamespace())
	dstMap, err := migration.LoadLogical(ctx, dst, req.GetRoots())
	if err != nil {
		return &catalogpb.VerifyImportResponse{Status: merr.Status(err)}, nil
	}
	srcMap := make(map[string]string, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		srcMap[e.GetKey()] = string(e.GetValue())
	}
	mismatches := migration.DiffMaps(srcMap, dstMap)
	resp := &catalogpb.VerifyImportResponse{Status: merr.Success()}
	for _, m := range mismatches {
		resp.Mismatches = append(resp.Mismatches, &catalogpb.VerifyMismatch{Key: m.Key, Reason: m.Reason})
	}
	return resp, nil
}
