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

package meta

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	coordmeta "github.com/milvus-io/milvus/pkg/v3/coordmeta/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// The Replica / ReplicaManager / ResourceGroup / ResourceManager implementations
// now live in the shared pkg/v3/coordmeta/querycoord package so the pooled
// catalog service can reuse the same code. These aliases and thin constructor
// wrappers keep the existing internal/querycoordv2 call sites compiling
// unchanged.
//
// The moved code depends on two internal-only concepts through narrow pkg
// interfaces: the live-node registry (session.NodeManager / session.NodeInfo,
// bridged by nodeManagerAdapter below) and the reverse *Meta collection lookup
// (coordmeta.ReplicaCollectionProvider, which the internal *Meta already
// satisfies structurally via its embedded *CollectionManager).
type (
	Replica                      = coordmeta.Replica
	ReplicaInterface             = coordmeta.ReplicaInterface
	ReplicaManager               = coordmeta.ReplicaManager
	ReplicaManagerInterface      = coordmeta.ReplicaManagerInterface
	SpawnOption                  = coordmeta.SpawnOption
	SpawnWithReplicaConfigParams = coordmeta.SpawnWithReplicaConfigParams
	ResourceManager              = coordmeta.ResourceManager
	ResourceGroup                = coordmeta.ResourceGroup
)

const (
	DefaultResourceGroupName = coordmeta.DefaultResourceGroupName

	RoundRobinBalancerName        = coordmeta.RoundRobinBalancerName
	RowCountBasedBalancerName     = coordmeta.RowCountBasedBalancerName
	ScoreBasedBalancerName        = coordmeta.ScoreBasedBalancerName
	MultiTargetBalancerName       = coordmeta.MultiTargetBalancerName
	ChannelLevelScoreBalancerName = coordmeta.ChannelLevelScoreBalancerName
)

var (
	NilReplica = coordmeta.NilReplica

	NewReplica             = coordmeta.NewReplica
	NewReplicaWithPriority = coordmeta.NewReplicaWithPriority
	NewReplicaManager      = coordmeta.NewReplicaManager
	NewResourceGroupConfig = coordmeta.NewResourceGroupConfig
	WithNeedWaitRGReady    = coordmeta.WithNeedWaitRGReady
	WithQueryInvisible     = coordmeta.WithQueryInvisible
)

// NewResourceGroup constructs a resource group, adapting the internal
// *session.NodeManager into the pkg NodeManager interface.
func NewResourceGroup(name string, cfg *rgpb.ResourceGroupConfig, nodeMgr *session.NodeManager) *ResourceGroup {
	return coordmeta.NewResourceGroup(name, cfg, newNodeManagerAdapter(nodeMgr))
}

// NewResourceGroupFromMeta constructs a resource group from persisted meta,
// adapting the internal *session.NodeManager into the pkg NodeManager interface.
func NewResourceGroupFromMeta(meta *querypb.ResourceGroup, nodeMgr *session.NodeManager) *ResourceGroup {
	return coordmeta.NewResourceGroupFromMeta(meta, newNodeManagerAdapter(nodeMgr))
}

// NewResourceManager constructs a ResourceManager, adapting the internal
// *session.NodeManager into the pkg NodeManager interface.
func NewResourceManager(catalog metastore.QueryCoordCatalog, nodeMgr *session.NodeManager) *ResourceManager {
	return coordmeta.NewResourceManager(catalog, newNodeManagerAdapter(nodeMgr))
}

// nodeManagerAdapter bridges *session.NodeManager to the coordmeta.NodeManager
// interface expected by the moved ResourceManager / ResourceGroup.
type nodeManagerAdapter struct {
	inner *session.NodeManager
}

// newNodeManagerAdapter wraps a *session.NodeManager. A nil manager maps to a
// nil interface so downstream nil-manager handling is preserved.
func newNodeManagerAdapter(nodeMgr *session.NodeManager) coordmeta.NodeManager {
	if nodeMgr == nil {
		return nil
	}
	return &nodeManagerAdapter{inner: nodeMgr}
}

// Get returns the node info, or a true nil interface when the node is absent.
// The explicit nil check avoids boxing a nil *session.NodeInfo into a non-nil
// coordmeta.NodeInfo, which would break the callers' `nodeInfo == nil` guards.
func (a *nodeManagerAdapter) Get(nodeID int64) coordmeta.NodeInfo {
	ni := a.inner.Get(nodeID)
	if ni == nil {
		return nil
	}
	return ni
}

// GetAll returns all live nodes. session.NodeManager.GetAll never yields nil
// entries, so each is boxed directly.
func (a *nodeManagerAdapter) GetAll() []coordmeta.NodeInfo {
	all := a.inner.GetAll()
	ret := make([]coordmeta.NodeInfo, 0, len(all))
	for _, ni := range all {
		ret = append(ret, ni)
	}
	return ret
}
