package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Privilege Group DDL/DCL ----

func (s *Server) CreatePrivilegeGroup(ctx context.Context, req *catalogpb.CreatePrivilegeGroupRequest) (*catalogpb.CreatePrivilegeGroupResponse, error) {
	err := s.meta.CreatePrivilegeGroup(ctx, req.GetGroupName())
	return &catalogpb.CreatePrivilegeGroupResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DropPrivilegeGroup(ctx context.Context, req *catalogpb.DropPrivilegeGroupRequest) (*catalogpb.DropPrivilegeGroupResponse, error) {
	err := s.meta.DropPrivilegeGroup(ctx, req.GetGroupName())
	return &catalogpb.DropPrivilegeGroupResponse{Status: merr.Status(err)}, nil
}

func (s *Server) OperatePrivilegeGroup(ctx context.Context, req *catalogpb.OperatePrivilegeGroupRequest) (*catalogpb.OperatePrivilegeGroupResponse, error) {
	err := s.meta.OperatePrivilegeGroup(ctx, req.GetGroupName(), req.GetPrivileges(), req.GetOperateType())
	return &catalogpb.OperatePrivilegeGroupResponse{Status: merr.Status(err)}, nil
}

func (s *Server) IsCustomPrivilegeGroup(ctx context.Context, req *catalogpb.IsCustomPrivilegeGroupRequest) (*catalogpb.IsCustomPrivilegeGroupResponse, error) {
	isCustom, err := s.meta.IsCustomPrivilegeGroup(ctx, req.GetGroupName())
	resp := &catalogpb.IsCustomPrivilegeGroupResponse{Status: merr.Status(err)}
	if err == nil {
		resp.IsCustom = isCustom
	}
	return resp, nil
}

func (s *Server) ListPrivilegeGroups(ctx context.Context, req *catalogpb.ListPrivilegeGroupsRequest) (*catalogpb.ListPrivilegeGroupsResponse, error) {
	groups, err := s.meta.ListPrivilegeGroups(ctx)
	resp := &catalogpb.ListPrivilegeGroupsResponse{Status: merr.Status(err)}
	if err == nil {
		resp.PrivilegeGroups = groups
	}
	return resp, nil
}

func (s *Server) GetPrivilegeGroupRoles(ctx context.Context, req *catalogpb.GetPrivilegeGroupRolesRequest) (*catalogpb.GetPrivilegeGroupRolesResponse, error) {
	roles, err := s.meta.GetPrivilegeGroupRoles(ctx, req.GetGroupName())
	resp := &catalogpb.GetPrivilegeGroupRolesResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Roles = roles
	}
	return resp, nil
}

func (s *Server) CheckIfPrivilegeGroupCreatable(ctx context.Context, req *catalogpb.CheckIfPrivilegeGroupCreatableRequest) (*catalogpb.CheckIfPrivilegeGroupCreatableResponse, error) {
	err := s.meta.CheckIfPrivilegeGroupCreatable(ctx, req.GetReq())
	return &catalogpb.CheckIfPrivilegeGroupCreatableResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfPrivilegeGroupAlterable(ctx context.Context, req *catalogpb.CheckIfPrivilegeGroupAlterableRequest) (*catalogpb.CheckIfPrivilegeGroupAlterableResponse, error) {
	err := s.meta.CheckIfPrivilegeGroupAlterable(ctx, req.GetReq())
	return &catalogpb.CheckIfPrivilegeGroupAlterableResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfPrivilegeGroupDropable(ctx context.Context, req *catalogpb.CheckIfPrivilegeGroupDropableRequest) (*catalogpb.CheckIfPrivilegeGroupDropableResponse, error) {
	err := s.meta.CheckIfPrivilegeGroupDropable(ctx, req.GetReq())
	return &catalogpb.CheckIfPrivilegeGroupDropableResponse{Status: merr.Status(err)}, nil
}
