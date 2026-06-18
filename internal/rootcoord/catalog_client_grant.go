package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- grant group ----

func (r *remoteMetaTable) OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	resp, err := r.client.OperatePrivilege(ctx, &catalogpb.OperatePrivilegeRequest{Tenant: tenant, Entity: entity, OperateType: operateType})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DropGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	resp, err := r.client.DropGrant(ctx, &catalogpb.DropGrantRequest{Tenant: tenant, Role: role})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	resp, err := r.client.RestoreRBAC(ctx, &catalogpb.RestoreRBACRequest{Tenant: tenant, Meta: meta})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	resp, err := r.client.SelectGrant(ctx, &catalogpb.SelectGrantRequest{Tenant: tenant, Entity: entity})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetEntities(), nil
}

func (r *remoteMetaTable) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	resp, err := r.client.ListPolicy(ctx, &catalogpb.ListPolicyRequest{Tenant: tenant})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetEntities(), nil
}

func (r *remoteMetaTable) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	resp, err := r.client.BackupRBAC(ctx, &catalogpb.BackupRBACRequest{Tenant: tenant})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetRbacMeta(), nil
}

func (r *remoteMetaTable) CheckIfRBACRestorable(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) error {
	resp, err := r.client.CheckIfRBACRestorable(ctx, &catalogpb.CheckIfRBACRestorableRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
