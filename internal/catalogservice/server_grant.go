package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- grant group ----

func (s *Server) OperatePrivilege(ctx context.Context, req *catalogpb.OperatePrivilegeRequest) (*catalogpb.OperatePrivilegeResponse, error) {
	err := s.meta.OperatePrivilege(ctx, req.GetTenant(), req.GetEntity(), req.GetOperateType())
	return &catalogpb.OperatePrivilegeResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DropGrant(ctx context.Context, req *catalogpb.DropGrantRequest) (*catalogpb.DropGrantResponse, error) {
	err := s.meta.DropGrant(ctx, req.GetTenant(), req.GetRole())
	return &catalogpb.DropGrantResponse{Status: merr.Status(err)}, nil
}

func (s *Server) RestoreRBAC(ctx context.Context, req *catalogpb.RestoreRBACRequest) (*catalogpb.RestoreRBACResponse, error) {
	err := s.meta.RestoreRBAC(ctx, req.GetTenant(), req.GetMeta())
	return &catalogpb.RestoreRBACResponse{Status: merr.Status(err)}, nil
}

func (s *Server) SelectGrant(ctx context.Context, req *catalogpb.SelectGrantRequest) (*catalogpb.SelectGrantResponse, error) {
	entities, err := s.meta.SelectGrant(ctx, req.GetTenant(), req.GetEntity())
	resp := &catalogpb.SelectGrantResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Entities = entities
	}
	return resp, nil
}

func (s *Server) ListPolicy(ctx context.Context, req *catalogpb.ListPolicyRequest) (*catalogpb.ListPolicyResponse, error) {
	entities, err := s.meta.ListPolicy(ctx, req.GetTenant())
	resp := &catalogpb.ListPolicyResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Entities = entities
	}
	return resp, nil
}

func (s *Server) BackupRBAC(ctx context.Context, req *catalogpb.BackupRBACRequest) (*catalogpb.BackupRBACResponse, error) {
	meta, err := s.meta.BackupRBAC(ctx, req.GetTenant())
	resp := &catalogpb.BackupRBACResponse{Status: merr.Status(err)}
	if err == nil {
		resp.RbacMeta = meta
	}
	return resp, nil
}

func (s *Server) CheckIfRBACRestorable(ctx context.Context, req *catalogpb.CheckIfRBACRestorableRequest) (*catalogpb.CheckIfRBACRestorableResponse, error) {
	err := s.meta.CheckIfRBACRestorable(ctx, req.GetReq())
	return &catalogpb.CheckIfRBACRestorableResponse{Status: merr.Status(err)}, nil
}
