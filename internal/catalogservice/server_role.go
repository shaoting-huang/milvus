package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Role / User-Role DCL ----

func (s *Server) CreateRole(ctx context.Context, req *catalogpb.CreateRoleRequest) (*catalogpb.CreateRoleResponse, error) {
	err := s.meta.CreateRole(ctx, req.GetTenant(), req.GetEntity())
	return &catalogpb.CreateRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) AlterRole(ctx context.Context, req *catalogpb.AlterRoleRequest) (*catalogpb.AlterRoleResponse, error) {
	err := s.meta.AlterRole(ctx, req.GetTenant(), req.GetEntity())
	return &catalogpb.AlterRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DropRole(ctx context.Context, req *catalogpb.DropRoleRequest) (*catalogpb.DropRoleResponse, error) {
	err := s.meta.DropRole(ctx, req.GetTenant(), req.GetRoleName())
	return &catalogpb.DropRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) OperateUserRole(ctx context.Context, req *catalogpb.OperateUserRoleRequest) (*catalogpb.OperateUserRoleResponse, error) {
	err := s.meta.OperateUserRole(ctx, req.GetTenant(), req.GetUserEntity(), req.GetRoleEntity(), req.GetOperateType())
	return &catalogpb.OperateUserRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) SelectRole(ctx context.Context, req *catalogpb.SelectRoleRequest) (*catalogpb.SelectRoleResponse, error) {
	results, err := s.meta.SelectRole(ctx, req.GetTenant(), req.GetEntity(), req.GetIncludeUserInfo())
	resp := &catalogpb.SelectRoleResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Results = results
	}
	return resp, nil
}

func (s *Server) SelectUser(ctx context.Context, req *catalogpb.SelectUserRequest) (*catalogpb.SelectUserResponse, error) {
	results, err := s.meta.SelectUser(ctx, req.GetTenant(), req.GetEntity(), req.GetIncludeRoleInfo())
	resp := &catalogpb.SelectUserResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Results = results
	}
	return resp, nil
}

func (s *Server) ListUserRole(ctx context.Context, req *catalogpb.ListUserRoleRequest) (*catalogpb.ListUserRoleResponse, error) {
	userRoles, err := s.meta.ListUserRole(ctx, req.GetTenant())
	resp := &catalogpb.ListUserRoleResponse{Status: merr.Status(err)}
	if err == nil {
		resp.UserRoles = userRoles
	}
	return resp, nil
}

func (s *Server) CheckIfCreateRole(ctx context.Context, req *catalogpb.CheckIfCreateRoleRequest) (*catalogpb.CheckIfCreateRoleResponse, error) {
	err := s.meta.CheckIfCreateRole(ctx, req.GetReq())
	return &catalogpb.CheckIfCreateRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfAlterRole(ctx context.Context, req *catalogpb.CheckIfAlterRoleRequest) (*catalogpb.CheckIfAlterRoleResponse, error) {
	err := s.meta.CheckIfAlterRole(ctx, req.GetReq())
	return &catalogpb.CheckIfAlterRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfDropRole(ctx context.Context, req *catalogpb.CheckIfDropRoleRequest) (*catalogpb.CheckIfDropRoleResponse, error) {
	err := s.meta.CheckIfDropRole(ctx, req.GetReq())
	return &catalogpb.CheckIfDropRoleResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfOperateUserRole(ctx context.Context, req *catalogpb.CheckIfOperateUserRoleRequest) (*catalogpb.CheckIfOperateUserRoleResponse, error) {
	err := s.meta.CheckIfOperateUserRole(ctx, req.GetReq())
	return &catalogpb.CheckIfOperateUserRoleResponse{Status: merr.Status(err)}, nil
}
