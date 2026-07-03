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

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReplicaCollectionProvider abstracts the single reverse lookup that
// ReplicaManager.GetReplicasJSON needs from the surrounding *Meta: given a
// collection id, return its loaded Collection (to read the database id for the
// metrics payload). Declaring it here keeps the moved ReplicaManager free of a
// reverse dependency on the internal *Meta aggregate. The internal *Meta
// structurally satisfies it via its embedded *CollectionManager.
type ReplicaCollectionProvider interface {
	GetCollection(ctx context.Context, collectionID typeutil.UniqueID) *Collection
}

// NodeManager abstracts the live-node registry (service discovery view) that
// ResourceManager / ResourceGroup consult. The production implementation lives
// in internal/querycoordv2/session (*session.NodeManager); an adapter in
// internal/querycoordv2/meta bridges it to this interface so the moved code
// stays free of the internal session package.
//
// Get returns nil when the node is not present; callers rely on the returned
// interface being a true nil (the adapter must not box a nil concrete pointer).
type NodeManager interface {
	Get(nodeID int64) NodeInfo
	GetAll() []NodeInfo
}

// NodeInfo abstracts the per-node state that ResourceManager / ResourceGroup
// read. It exposes only the methods those two managers actually call; the
// production *session.NodeInfo satisfies it structurally.
type NodeInfo interface {
	ID() int64
	ResourceGroupName() string
	Labels() map[string]string
	IsStoppingState() bool
	IsEmbeddedQueryNodeInStreamingNode() bool
}
