package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Privilege Group DDL/DCL ----

func (r *remoteMetaTable) CreatePrivilegeGroup(ctx context.Context, groupName string) error {
	resp, err := r.client.CreatePrivilegeGroup(ctx, &catalogpb.CreatePrivilegeGroupRequest{GroupName: groupName})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	resp, err := r.client.DropPrivilegeGroup(ctx, &catalogpb.DropPrivilegeGroupRequest{GroupName: groupName})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
	resp, err := r.client.OperatePrivilegeGroup(ctx, &catalogpb.OperatePrivilegeGroupRequest{
		GroupName:   groupName,
		Privileges:  privileges,
		OperateType: operateType,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) IsCustomPrivilegeGroup(ctx context.Context, groupName string) (bool, error) {
	resp, err := r.client.IsCustomPrivilegeGroup(ctx, &catalogpb.IsCustomPrivilegeGroupRequest{GroupName: groupName})
	if err != nil {
		return false, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return false, err
	}
	return resp.GetIsCustom(), nil
}

func (r *remoteMetaTable) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	resp, err := r.client.ListPrivilegeGroups(ctx, &catalogpb.ListPrivilegeGroupsRequest{})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetPrivilegeGroups(), nil
}

func (r *remoteMetaTable) GetPrivilegeGroupRoles(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error) {
	resp, err := r.client.GetPrivilegeGroupRoles(ctx, &catalogpb.GetPrivilegeGroupRolesRequest{GroupName: groupName})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetRoles(), nil
}

func (r *remoteMetaTable) CheckIfPrivilegeGroupCreatable(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) error {
	resp, err := r.client.CheckIfPrivilegeGroupCreatable(ctx, &catalogpb.CheckIfPrivilegeGroupCreatableRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfPrivilegeGroupAlterable(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) error {
	resp, err := r.client.CheckIfPrivilegeGroupAlterable(ctx, &catalogpb.CheckIfPrivilegeGroupAlterableRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfPrivilegeGroupDropable(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) error {
	resp, err := r.client.CheckIfPrivilegeGroupDropable(ctx, &catalogpb.CheckIfPrivilegeGroupDropableRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
