package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Role / User-Role DCL ----

func (r *remoteMetaTable) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	resp, err := r.client.CreateRole(ctx, &catalogpb.CreateRoleRequest{Tenant: tenant, Entity: entity})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	resp, err := r.client.AlterRole(ctx, &catalogpb.AlterRoleRequest{Tenant: tenant, Entity: entity})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DropRole(ctx context.Context, tenant string, roleName string) error {
	resp, err := r.client.DropRole(ctx, &catalogpb.DropRoleRequest{Tenant: tenant, RoleName: roleName})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	resp, err := r.client.OperateUserRole(ctx, &catalogpb.OperateUserRoleRequest{
		Tenant:      tenant,
		UserEntity:  userEntity,
		RoleEntity:  roleEntity,
		OperateType: operateType,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	resp, err := r.client.SelectRole(ctx, &catalogpb.SelectRoleRequest{Tenant: tenant, Entity: entity, IncludeUserInfo: includeUserInfo})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetResults(), nil
}

func (r *remoteMetaTable) SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	resp, err := r.client.SelectUser(ctx, &catalogpb.SelectUserRequest{Tenant: tenant, Entity: entity, IncludeRoleInfo: includeRoleInfo})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetResults(), nil
}

func (r *remoteMetaTable) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	resp, err := r.client.ListUserRole(ctx, &catalogpb.ListUserRoleRequest{Tenant: tenant})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetUserRoles(), nil
}

func (r *remoteMetaTable) CheckIfCreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) error {
	resp, err := r.client.CheckIfCreateRole(ctx, &catalogpb.CheckIfCreateRoleRequest{Req: in})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfAlterRole(ctx context.Context, in *milvuspb.AlterRoleRequest) error {
	resp, err := r.client.CheckIfAlterRole(ctx, &catalogpb.CheckIfAlterRoleRequest{Req: in})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	resp, err := r.client.CheckIfDropRole(ctx, &catalogpb.CheckIfDropRoleRequest{Req: in})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfOperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) error {
	resp, err := r.client.CheckIfOperateUserRole(ctx, &catalogpb.CheckIfOperateUserRoleRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
