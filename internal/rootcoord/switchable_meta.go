// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// switchableMetaTable is the coord-side IMetaTable wrapper that enables a runtime cutover of
// the metadata backend. c.meta points at this wrapper; every read/write delegates to the
// target held behind an atomic pointer. During migration Switch atomically swaps the target
// from the local *MetaTable to a remote client (or a blocking meta) with NO data race against
// the many naked c.meta read sites — readers only ever load the pointer, never mutate it.
type switchableMetaTable struct {
	// target holds the active IMetaTable. atomic.Pointer gives lock-free reads on the hot path
	// (the ~45 c.meta delegations) and a single atomic store on the cold path (Switch).
	target atomic.Pointer[imetaHolder]
}

// imetaHolder boxes an IMetaTable so it can live inside atomic.Pointer (which requires a
// concrete pointer type). The interface value is immutable once stored.
type imetaHolder struct {
	mt IMetaTable
}

// compile-time proof that the wrapper satisfies the full IMetaTable surface.
var _ IMetaTable = (*switchableMetaTable)(nil)

// NewSwitchableMetaTable wires a coord-side IMetaTable whose target can be hot-swapped at
// runtime via Switch. initial is the backend used until the first Switch.
func NewSwitchableMetaTable(initial IMetaTable) *switchableMetaTable {
	s := &switchableMetaTable{}
	s.target.Store(&imetaHolder{mt: initial})
	return s
}

// Switch atomically replaces the active target. In-flight delegations already routed to the
// previous target complete against it; subsequent calls route to the new one.
func (s *switchableMetaTable) Switch(target IMetaTable) {
	s.target.Store(&imetaHolder{mt: target})
}

// Current returns the active target.
func (s *switchableMetaTable) Current() IMetaTable {
	return s.target.Load().mt
}

// current is the hot-path accessor every delegation funnels through.
func (s *switchableMetaTable) current() IMetaTable {
	return s.target.Load().mt
}

// ---- MetaTableChecker ----

func (s *switchableMetaTable) CheckIfDatabaseCreatable(ctx context.Context, req *milvuspb.CreateDatabaseRequest) error {
	return s.current().CheckIfDatabaseCreatable(ctx, req)
}

func (s *switchableMetaTable) CheckIfDatabaseDroppable(ctx context.Context, req *milvuspb.DropDatabaseRequest) error {
	return s.current().CheckIfDatabaseDroppable(ctx, req)
}

func (s *switchableMetaTable) CheckIfAliasCreatable(ctx context.Context, dbName string, alias string, collectionName string) error {
	return s.current().CheckIfAliasCreatable(ctx, dbName, alias, collectionName)
}

func (s *switchableMetaTable) CheckIfAliasAlterable(ctx context.Context, dbName string, alias string, collectionName string) error {
	return s.current().CheckIfAliasAlterable(ctx, dbName, alias, collectionName)
}

func (s *switchableMetaTable) CheckIfAliasDroppable(ctx context.Context, dbName string, alias string) error {
	return s.current().CheckIfAliasDroppable(ctx, dbName, alias)
}

// ---- RBACChecker ----

func (s *switchableMetaTable) CheckIfAddCredential(ctx context.Context, req *internalpb.CredentialInfo) error {
	return s.current().CheckIfAddCredential(ctx, req)
}

func (s *switchableMetaTable) CheckIfUpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) error {
	return s.current().CheckIfUpdateCredential(ctx, req)
}

func (s *switchableMetaTable) CheckIfDeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) error {
	return s.current().CheckIfDeleteCredential(ctx, req)
}

func (s *switchableMetaTable) CheckIfCreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) error {
	return s.current().CheckIfCreateRole(ctx, req)
}

func (s *switchableMetaTable) CheckIfAlterRole(ctx context.Context, req *milvuspb.AlterRoleRequest) error {
	return s.current().CheckIfAlterRole(ctx, req)
}

func (s *switchableMetaTable) CheckIfDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	return s.current().CheckIfDropRole(ctx, in)
}

func (s *switchableMetaTable) CheckIfOperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) error {
	return s.current().CheckIfOperateUserRole(ctx, req)
}

func (s *switchableMetaTable) CheckIfPrivilegeGroupCreatable(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) error {
	return s.current().CheckIfPrivilegeGroupCreatable(ctx, req)
}

func (s *switchableMetaTable) CheckIfPrivilegeGroupAlterable(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) error {
	return s.current().CheckIfPrivilegeGroupAlterable(ctx, req)
}

func (s *switchableMetaTable) CheckIfPrivilegeGroupDropable(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) error {
	return s.current().CheckIfPrivilegeGroupDropable(ctx, req)
}

func (s *switchableMetaTable) CheckIfRBACRestorable(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) error {
	return s.current().CheckIfRBACRestorable(ctx, req)
}

// ---- Database DDL ----

func (s *switchableMetaTable) GetDatabaseByID(ctx context.Context, dbID int64, ts Timestamp) (*model.Database, error) {
	return s.current().GetDatabaseByID(ctx, dbID, ts)
}

func (s *switchableMetaTable) GetDatabaseByName(ctx context.Context, dbName string, ts Timestamp) (*model.Database, error) {
	return s.current().GetDatabaseByName(ctx, dbName, ts)
}

func (s *switchableMetaTable) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	return s.current().CreateDatabase(ctx, db, ts)
}

func (s *switchableMetaTable) DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error {
	return s.current().DropDatabase(ctx, dbName, ts)
}

func (s *switchableMetaTable) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	return s.current().ListDatabases(ctx, ts)
}

func (s *switchableMetaTable) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	return s.current().AlterDatabase(ctx, newDB, ts)
}

// ---- Collection / Partition DDL ----

func (s *switchableMetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	return s.current().AddCollection(ctx, coll)
}

func (s *switchableMetaTable) DropCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	return s.current().DropCollection(ctx, collectionID, ts)
}

func (s *switchableMetaTable) RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	return s.current().RemoveCollection(ctx, collectionID, ts)
}

func (s *switchableMetaTable) GetCollectionID(ctx context.Context, dbName string, collectionName string) UniqueID {
	return s.current().GetCollectionID(ctx, dbName, collectionName)
}

func (s *switchableMetaTable) GetCollectionByName(ctx context.Context, dbName string, collectionName string, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	return s.current().GetCollectionByName(ctx, dbName, collectionName, ts, allowUnavailable)
}

func (s *switchableMetaTable) GetCollectionByID(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	return s.current().GetCollectionByID(ctx, dbName, collectionID, ts, allowUnavailable)
}

func (s *switchableMetaTable) GetCollectionByIDWithMaxTs(ctx context.Context, collectionID UniqueID) (*model.Collection, error) {
	return s.current().GetCollectionByIDWithMaxTs(ctx, collectionID)
}

func (s *switchableMetaTable) ListCollections(ctx context.Context, dbName string, ts Timestamp, onlyAvail bool) ([]*model.Collection, error) {
	return s.current().ListCollections(ctx, dbName, ts, onlyAvail)
}

func (s *switchableMetaTable) ListAllAvailCollections(ctx context.Context) map[int64][]int64 {
	return s.current().ListAllAvailCollections(ctx)
}

func (s *switchableMetaTable) ListAllAvailPartitions(ctx context.Context) map[int64]map[int64][]int64 {
	return s.current().ListAllAvailPartitions(ctx)
}

func (s *switchableMetaTable) ListCollectionPhysicalChannels(ctx context.Context) map[typeutil.UniqueID][]string {
	return s.current().ListCollectionPhysicalChannels(ctx)
}

func (s *switchableMetaTable) GetCollectionVirtualChannels(ctx context.Context, colID int64) []string {
	return s.current().GetCollectionVirtualChannels(ctx, colID)
}

func (s *switchableMetaTable) GetPChannelInfo(ctx context.Context, pchannel string) *rootcoordpb.GetPChannelInfoResponse {
	return s.current().GetPChannelInfo(ctx, pchannel)
}

func (s *switchableMetaTable) AddPartition(ctx context.Context, partition *model.Partition) error {
	return s.current().AddPartition(ctx, partition)
}

func (s *switchableMetaTable) GetPartitionIDByName(collectionID int64, partitionName string) (int64, bool) {
	return s.current().GetPartitionIDByName(collectionID, partitionName)
}

func (s *switchableMetaTable) DropPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	return s.current().DropPartition(ctx, collectionID, partitionID, ts)
}

func (s *switchableMetaTable) RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	return s.current().RemovePartition(ctx, collectionID, partitionID, ts)
}

// ---- Alias ----

func (s *switchableMetaTable) AlterAlias(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error {
	return s.current().AlterAlias(ctx, result)
}

func (s *switchableMetaTable) DropAlias(ctx context.Context, result message.BroadcastResultDropAliasMessageV2) error {
	return s.current().DropAlias(ctx, result)
}

func (s *switchableMetaTable) DescribeAlias(ctx context.Context, dbName string, alias string, ts Timestamp) (string, error) {
	return s.current().DescribeAlias(ctx, dbName, alias, ts)
}

func (s *switchableMetaTable) ListAliases(ctx context.Context, dbName string, collectionName string, ts Timestamp) ([]string, error) {
	return s.current().ListAliases(ctx, dbName, collectionName, ts)
}

func (s *switchableMetaTable) AlterCollection(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	return s.current().AlterCollection(ctx, result)
}

func (s *switchableMetaTable) BeginTruncateCollection(ctx context.Context, collectionID UniqueID) error {
	return s.current().BeginTruncateCollection(ctx, collectionID)
}

func (s *switchableMetaTable) TruncateCollection(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error {
	return s.current().TruncateCollection(ctx, result)
}

func (s *switchableMetaTable) CheckIfCollectionRenamable(ctx context.Context, dbName string, oldName string, newDBName string, newName string) error {
	return s.current().CheckIfCollectionRenamable(ctx, dbName, oldName, newDBName, newName)
}

func (s *switchableMetaTable) GetGeneralCount(ctx context.Context) int {
	return s.current().GetGeneralCount(ctx)
}

func (s *switchableMetaTable) IsAlias(ctx context.Context, db, name string) bool {
	return s.current().IsAlias(ctx, db, name)
}

func (s *switchableMetaTable) ListAliasesByID(ctx context.Context, collID UniqueID) []string {
	return s.current().ListAliasesByID(ctx, collID)
}

// ---- Credential ----

func (s *switchableMetaTable) GetCredential(ctx context.Context, username string) (*internalpb.CredentialInfo, error) {
	return s.current().GetCredential(ctx, username)
}

func (s *switchableMetaTable) InitCredential(ctx context.Context) error {
	return s.current().InitCredential(ctx)
}

func (s *switchableMetaTable) DeleteCredential(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error {
	return s.current().DeleteCredential(ctx, result)
}

func (s *switchableMetaTable) AlterCredential(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error {
	return s.current().AlterCredential(ctx, result)
}

func (s *switchableMetaTable) ListCredentialUsernames(ctx context.Context) (*milvuspb.ListCredUsersResponse, error) {
	return s.current().ListCredentialUsernames(ctx)
}

// ---- RBAC ----

func (s *switchableMetaTable) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	return s.current().CreateRole(ctx, tenant, entity)
}

func (s *switchableMetaTable) AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	return s.current().AlterRole(ctx, tenant, entity)
}

func (s *switchableMetaTable) DropRole(ctx context.Context, tenant string, roleName string) error {
	return s.current().DropRole(ctx, tenant, roleName)
}

func (s *switchableMetaTable) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	return s.current().OperateUserRole(ctx, tenant, userEntity, roleEntity, operateType)
}

func (s *switchableMetaTable) SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	return s.current().SelectRole(ctx, tenant, entity, includeUserInfo)
}

func (s *switchableMetaTable) SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	return s.current().SelectUser(ctx, tenant, entity, includeRoleInfo)
}

func (s *switchableMetaTable) OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	return s.current().OperatePrivilege(ctx, tenant, entity, operateType)
}

func (s *switchableMetaTable) SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	return s.current().SelectGrant(ctx, tenant, entity)
}

func (s *switchableMetaTable) DropGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	return s.current().DropGrant(ctx, tenant, role)
}

func (s *switchableMetaTable) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	return s.current().ListPolicy(ctx, tenant)
}

func (s *switchableMetaTable) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	return s.current().ListUserRole(ctx, tenant)
}

func (s *switchableMetaTable) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	return s.current().BackupRBAC(ctx, tenant)
}

func (s *switchableMetaTable) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	return s.current().RestoreRBAC(ctx, tenant, meta)
}

func (s *switchableMetaTable) IsCustomPrivilegeGroup(ctx context.Context, groupName string) (bool, error) {
	return s.current().IsCustomPrivilegeGroup(ctx, groupName)
}

func (s *switchableMetaTable) CreatePrivilegeGroup(ctx context.Context, groupName string) error {
	return s.current().CreatePrivilegeGroup(ctx, groupName)
}

func (s *switchableMetaTable) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	return s.current().DropPrivilegeGroup(ctx, groupName)
}

func (s *switchableMetaTable) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	return s.current().ListPrivilegeGroups(ctx)
}

func (s *switchableMetaTable) OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
	return s.current().OperatePrivilegeGroup(ctx, groupName, privileges, operateType)
}

func (s *switchableMetaTable) GetPrivilegeGroupRoles(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error) {
	return s.current().GetPrivilegeGroupRoles(ctx, groupName)
}

// ---- File resource ----

func (s *switchableMetaTable) AddFileResource(ctx context.Context, resource *internalpb.FileResourceInfo) error {
	return s.current().AddFileResource(ctx, resource)
}

func (s *switchableMetaTable) RemoveFileResource(ctx context.Context, name string) (error, bool) {
	return s.current().RemoveFileResource(ctx, name)
}

func (s *switchableMetaTable) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64) {
	return s.current().ListFileResource(ctx)
}

func (s *switchableMetaTable) IncFileResourceRefCnt(ids []int64) error {
	return s.current().IncFileResourceRefCnt(ids)
}

func (s *switchableMetaTable) DecFileResourceRefCnt(ids []int64) {
	s.current().DecFileResourceRefCnt(ids)
}

func (s *switchableMetaTable) RecoverFileResourceRefCnt(pendingCollections map[int64][]int64) {
	s.current().RecoverFileResourceRefCnt(pendingCollections)
}
