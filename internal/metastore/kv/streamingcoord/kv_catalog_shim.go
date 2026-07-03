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

// Package streamingcoord now only re-exports the KV-backed StreamingCoord
// Catalog implementation that lives in the shared pkg/v3/metastore/kv/streamingcoord
// package. Moving the single implementation into pkg/v3 lets the pooled catalog
// service (an independent module) construct a TiKV-backed StreamingCoord catalog
// from the exact same code milvus standalone uses — one implementation, no fork.
// These aliases keep every existing internal/metastore/kv/streamingcoord import
// site (internal/streamingcoord/server/builder.go and internal/cdc/controller)
// working unchanged.
package streamingcoord

import (
	kvstreamingcoord "github.com/milvus-io/milvus/pkg/v3/metastore/kv/streamingcoord"
)

// Meta key-space prefixes / keys re-exported from
// pkg/v3/metastore/kv/streamingcoord.
const (
	MetaPrefix          = kvstreamingcoord.MetaPrefix
	PChannelMetaPrefix  = kvstreamingcoord.PChannelMetaPrefix
	BroadcastTaskPrefix = kvstreamingcoord.BroadcastTaskPrefix
	VersionKey          = kvstreamingcoord.VersionKey
	CChannelMetaKey     = kvstreamingcoord.CChannelMetaKey

	// Replicate
	ReplicatePChannelMetaPrefix = kvstreamingcoord.ReplicatePChannelMetaPrefix
	ReplicateConfigurationKey   = kvstreamingcoord.ReplicateConfigurationKey
)

// Re-exported constructor and key builder.
var (
	NewCataLog                    = kvstreamingcoord.NewCataLog
	BuildReplicatePChannelMetaKey = kvstreamingcoord.BuildReplicatePChannelMetaKey
)
