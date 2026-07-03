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

// Package streamingnode now only re-exports the KV-backed StreamingNode Catalog
// implementation that lives in the shared pkg/v3/metastore/kv/streamingnode
// package. Moving the single implementation into pkg/v3 lets the pooled catalog
// service (an independent module) construct a TiKV-backed StreamingNode catalog
// from the exact same code milvus standalone uses — one implementation, no fork.
// These aliases keep the existing internal/metastore/kv/streamingnode import site
// (internal/streamingnode/server/builder.go) working unchanged.
package streamingnode

import (
	kvstreamingnode "github.com/milvus-io/milvus/pkg/v3/metastore/kv/streamingnode"
)

// Meta key-space prefixes / directory segments re-exported from
// pkg/v3/metastore/kv/streamingnode.
const (
	MetaPrefix = kvstreamingnode.MetaPrefix

	DirectoryWAL           = kvstreamingnode.DirectoryWAL
	DirectorySegmentAssign = kvstreamingnode.DirectorySegmentAssign
	DirectoryVChannel      = kvstreamingnode.DirectoryVChannel
	DirectorySchema        = kvstreamingnode.DirectorySchema

	KeyConsumeCheckpoint = kvstreamingnode.KeyConsumeCheckpoint
	KeySalvageCheckpoint = kvstreamingnode.KeySalvageCheckpoint
)

// Re-exported constructor.
var NewCataLog = kvstreamingnode.NewCataLog
