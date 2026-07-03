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

package querycoord

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// This file provides pkg-local test doubles that replace the internal-only
// dependencies the replica/resource manager tests used before these
// implementations were moved into the shared pkg module:
//
//   - session.NodeManager / session.NodeInfo  -> fakeNodeManager / fakeNodeInfo
//     (implement the narrow NodeManager / NodeInfo interfaces this package
//     declares, so no internal/querycoordv2/session import is needed).
//   - the etcd-backed QueryCoordCatalog          -> fakeCatalog, an in-memory,
//     stateful store that faithfully reproduces the save/get/release round-trip
//     the recover-path tests rely on (a scripted mockery mock cannot emulate
//     save->recover round-trips without exactly this kind of map backing).
//   - the params id allocators                    -> randomIncrementIDAllocator /
//     errorIDAllocator.
//
// Label keys mirror internal/util/sessionutil so ResourceGroupName() /
// IsEmbeddedQueryNodeInStreamingNode() behave identically.

const (
	labelResourceGroup                        = "RESOURCE_GROUP"
	labelStreamingNodeEmbeddedQueryNode       = "STREAMING-EMBEDDED"
	legacyLabelStreamingNodeEmbeddedQueryNode = "QUERYNODE_" + labelStreamingNodeEmbeddedQueryNode
)

// fakeNodeState mirrors session.State for the two states the tests exercise.
type fakeNodeState int

const (
	fakeNodeStateNormal fakeNodeState = iota
	fakeNodeStateStopping
)

// fakeImmutableNodeInfo mirrors session.ImmutableNodeInfo (only the fields the
// tests set). Address/Hostname are kept so struct literals stay unchanged even
// though the NodeInfo interface does not read them.
type fakeImmutableNodeInfo struct {
	NodeID   int64
	Address  string
	Hostname string
	Labels   map[string]string
}

// fakeNodeInfo implements the NodeInfo interface.
type fakeNodeInfo struct {
	mu            sync.RWMutex
	immutableInfo fakeImmutableNodeInfo
	state         fakeNodeState
}

var _ NodeInfo = (*fakeNodeInfo)(nil)

func newFakeNodeInfo(info fakeImmutableNodeInfo) *fakeNodeInfo {
	return &fakeNodeInfo{immutableInfo: info}
}

func (n *fakeNodeInfo) ID() int64 {
	return n.immutableInfo.NodeID
}

func (n *fakeNodeInfo) Labels() map[string]string {
	return n.immutableInfo.Labels
}

func (n *fakeNodeInfo) ResourceGroupName() string {
	return n.immutableInfo.Labels[labelResourceGroup]
}

func (n *fakeNodeInfo) IsEmbeddedQueryNodeInStreamingNode() bool {
	return n.immutableInfo.Labels[labelStreamingNodeEmbeddedQueryNode] == "1" ||
		n.immutableInfo.Labels[legacyLabelStreamingNodeEmbeddedQueryNode] == "1"
}

func (n *fakeNodeInfo) IsStoppingState() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == fakeNodeStateStopping
}

func (n *fakeNodeInfo) SetState(s fakeNodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = s
}

// fakeNodeManager implements the NodeManager interface plus the Add/Remove
// helpers the tests use to mutate the live-node view.
type fakeNodeManager struct {
	mu    sync.RWMutex
	nodes map[int64]*fakeNodeInfo
}

var _ NodeManager = (*fakeNodeManager)(nil)

func newFakeNodeManager() *fakeNodeManager {
	return &fakeNodeManager{nodes: make(map[int64]*fakeNodeInfo)}
}

func (m *fakeNodeManager) Add(node *fakeNodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID()] = node
}

func (m *fakeNodeManager) Remove(nodeID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

// Get returns a true nil interface when the node is absent, matching the
// production adapter's nil-interface contract that AcceptNode / handleNodeUp
// rely on.
func (m *fakeNodeManager) Get(nodeID int64) NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if n, ok := m.nodes[nodeID]; ok {
		return n
	}
	return nil
}

func (m *fakeNodeManager) GetAll() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		ret = append(ret, n)
	}
	return ret
}

// fakeCatalog is an in-memory, stateful QueryCoordCatalog test double. Only the
// replica and resource-group methods the managers exercise carry real behavior;
// the remaining methods are inert. GetReplicas / GetResourceGroups return clones
// so callers that mutate recovered protos (e.g. Recover defaulting an empty
// resource group) never corrupt the stored state, matching etcd semantics.
type fakeCatalog struct {
	mu sync.Mutex

	replicas       map[int64]*querypb.Replica        // replicaID -> replica
	resourceGroups map[string]*querypb.ResourceGroup // name -> rg

	// saveReplicaErr / saveResourceGroupErr, when set, make the corresponding
	// Save* fail — used to exercise persistence-failure paths.
	saveReplicaErr       error
	saveResourceGroupErr error
}

var _ metastore.QueryCoordCatalog = (*fakeCatalog)(nil)

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{
		replicas:       make(map[int64]*querypb.Replica),
		resourceGroups: make(map[string]*querypb.ResourceGroup),
	}
}

// --- replica methods (stateful) ---

func (c *fakeCatalog) SaveReplica(_ context.Context, replicas ...*querypb.Replica) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveReplicaErr != nil {
		return c.saveReplicaErr
	}
	for _, r := range replicas {
		c.replicas[r.GetID()] = proto.Clone(r).(*querypb.Replica)
	}
	return nil
}

func (c *fakeCatalog) GetReplicas(_ context.Context) ([]*querypb.Replica, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make([]*querypb.Replica, 0, len(c.replicas))
	for _, r := range c.replicas {
		ret = append(ret, proto.Clone(r).(*querypb.Replica))
	}
	return ret, nil
}

func (c *fakeCatalog) ReleaseReplicas(_ context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, r := range c.replicas {
		if r.GetCollectionID() == collectionID {
			delete(c.replicas, id)
		}
	}
	return nil
}

func (c *fakeCatalog) ReleaseReplica(_ context.Context, collection int64, replicas ...int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range replicas {
		if r, ok := c.replicas[id]; ok && r.GetCollectionID() == collection {
			delete(c.replicas, id)
		}
	}
	return nil
}

// --- resource-group methods (stateful) ---

func (c *fakeCatalog) SaveResourceGroup(_ context.Context, rgs ...*querypb.ResourceGroup) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveResourceGroupErr != nil {
		return c.saveResourceGroupErr
	}
	for _, rg := range rgs {
		c.resourceGroups[rg.GetName()] = proto.Clone(rg).(*querypb.ResourceGroup)
	}
	return nil
}

func (c *fakeCatalog) RemoveResourceGroup(_ context.Context, rgName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.resourceGroups, rgName)
	return nil
}

func (c *fakeCatalog) GetResourceGroups(_ context.Context) ([]*querypb.ResourceGroup, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make([]*querypb.ResourceGroup, 0, len(c.resourceGroups))
	for _, rg := range c.resourceGroups {
		ret = append(ret, proto.Clone(rg).(*querypb.ResourceGroup))
	}
	return ret, nil
}

// --- inert methods (unused by these tests) ---

func (c *fakeCatalog) SaveCollection(_ context.Context, _ *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) error {
	return nil
}

func (c *fakeCatalog) SavePartition(_ context.Context, _ ...*querypb.PartitionLoadInfo) error {
	return nil
}

func (c *fakeCatalog) GetCollections(_ context.Context) ([]*querypb.CollectionLoadInfo, error) {
	return nil, nil
}

func (c *fakeCatalog) GetPartitions(_ context.Context, _ []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
	return nil, nil
}

func (c *fakeCatalog) ReleaseCollection(_ context.Context, _ int64) error {
	return nil
}

func (c *fakeCatalog) ReleasePartition(_ context.Context, _ int64, _ ...int64) error {
	return nil
}

func (c *fakeCatalog) SaveCollectionTargets(_ context.Context, _ ...*querypb.CollectionTarget) error {
	return nil
}

func (c *fakeCatalog) RemoveCollectionTarget(_ context.Context, _ int64) error {
	return nil
}

func (c *fakeCatalog) RemoveCollectionTargets(_ context.Context) error {
	return nil
}

func (c *fakeCatalog) GetCollectionTargets(_ context.Context) (map[int64]*querypb.CollectionTarget, error) {
	return nil, nil
}

// randomIncrementIDAllocator mirrors params.RandomIncrementIDAllocator.
func randomIncrementIDAllocator() func() (int64, error) {
	var id int64
	return func() (int64, error) {
		return atomic.AddInt64(&id, 1), nil
	}
}

// errFailedAllocateID is returned by errorIDAllocator; plain errors.New so
// errors.Is matches by identity.
var errFailedAllocateID = errors.New("failed to allocate ID")

// errorIDAllocator mirrors params.ErrorIDAllocator.
func errorIDAllocator() func() (int64, error) {
	return func() (int64, error) {
		return 0, errFailedAllocateID
	}
}
