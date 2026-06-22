package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// blockingMetaTable is the write gate used during a runtime catalog migration. It embeds
// the source MetaTable, so reads pass straight through (the source is still authoritative
// until cutover), but every mutation is rejected with a RETRIABLE error — the proxy retries
// and succeeds once migration finishes and c.meta is switched to the new backend. New writes
// are held off; in-flight writes that started before the gate was installed are drained by
// the orchestrator before copying.
type blockingMetaTable struct {
	IMetaTable // source meta — serves the (pass-through) reads
}

func newBlockingMetaTable(source IMetaTable) IMetaTable {
	return &blockingMetaTable{IMetaTable: source}
}

func blocked() error {
	return merr.WrapErrServiceUnavailable("catalog metadata migration in progress")
}

// ---- write methods: rejected with a retriable error during migration ----

func (b *blockingMetaTable) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	return blocked()
}
func (b *blockingMetaTable) DropCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) AlterCollection(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	return blocked()
}
func (b *blockingMetaTable) BeginTruncateCollection(ctx context.Context, collectionID UniqueID) error {
	return blocked()
}
func (b *blockingMetaTable) TruncateCollection(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error {
	return blocked()
}
func (b *blockingMetaTable) AddPartition(ctx context.Context, partition *model.Partition) error {
	return blocked()
}
func (b *blockingMetaTable) DropPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	return blocked()
}
func (b *blockingMetaTable) AlterAlias(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error {
	return blocked()
}
func (b *blockingMetaTable) DropAlias(ctx context.Context, result message.BroadcastResultDropAliasMessageV2) error {
	return blocked()
}
func (b *blockingMetaTable) InitCredential(ctx context.Context) error {
	return blocked()
}
func (b *blockingMetaTable) AlterCredential(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error {
	return blocked()
}
func (b *blockingMetaTable) DeleteCredential(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error {
	return blocked()
}
func (b *blockingMetaTable) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	return blocked()
}
func (b *blockingMetaTable) AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	return blocked()
}
func (b *blockingMetaTable) DropRole(ctx context.Context, tenant string, roleName string) error {
	return blocked()
}
func (b *blockingMetaTable) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	return blocked()
}
func (b *blockingMetaTable) OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	return blocked()
}
func (b *blockingMetaTable) DropGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	return blocked()
}
func (b *blockingMetaTable) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	return blocked()
}
func (b *blockingMetaTable) CreatePrivilegeGroup(ctx context.Context, groupName string) error {
	return blocked()
}
func (b *blockingMetaTable) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	return blocked()
}
func (b *blockingMetaTable) OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
	return blocked()
}
func (b *blockingMetaTable) AddFileResource(ctx context.Context, resource *internalpb.FileResourceInfo) error {
	return blocked()
}
func (b *blockingMetaTable) RemoveFileResource(ctx context.Context, name string) (error, bool) {
	return blocked(), false
}
func (b *blockingMetaTable) IncFileResourceRefCnt(ids []int64) error {
	return blocked()
}
