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

// Package querycoord now only re-exports the KV-backed QueryCoord Catalog
// implementation that lives in the shared pkg/v3/metastore/kv/querycoord
// package. Moving the single implementation into pkg/v3 lets the pooled catalog
// service (an independent module) construct a TiKV-backed QueryCoord catalog
// from the exact same code milvus standalone uses — one implementation, no fork.
// These aliases keep every existing internal/metastore/kv/querycoord import site
// (internal/querycoordv2/server.go, the querycoordv2 tests, and the migration
// tools under cmd/tools/migration) working unchanged.
package querycoord

import (
	kvquerycoord "github.com/milvus-io/milvus/pkg/v3/metastore/kv/querycoord"
)

// Catalog is the KV-backed QueryCoord catalog implementation.
type Catalog = kvquerycoord.Catalog

// Meta key-space prefixes / batch sizes re-exported from
// pkg/v3/metastore/kv/querycoord. The *V1 prefixes are the legacy 2.1 key spaces
// that getReplicasFromV1 and the migration tooling still read.
const (
	CollectionLoadInfoPrefix = kvquerycoord.CollectionLoadInfoPrefix
	PartitionLoadInfoPrefix  = kvquerycoord.PartitionLoadInfoPrefix
	ReplicaPrefix            = kvquerycoord.ReplicaPrefix
	CollectionMetaPrefixV1   = kvquerycoord.CollectionMetaPrefixV1
	ReplicaMetaPrefixV1      = kvquerycoord.ReplicaMetaPrefixV1
	ResourceGroupPrefix      = kvquerycoord.ResourceGroupPrefix

	MetaOpsBatchSize       = kvquerycoord.MetaOpsBatchSize
	CollectionTargetPrefix = kvquerycoord.CollectionTargetPrefix
)

// ErrInvalidKey is the sentinel returned for an unparseable load-info key.
var ErrInvalidKey = kvquerycoord.ErrInvalidKey

// Re-exported constructor and key builders.
var (
	NewCatalog = kvquerycoord.NewCatalog

	EncodeCollectionLoadInfoKey   = kvquerycoord.EncodeCollectionLoadInfoKey
	EncodePartitionLoadInfoKey    = kvquerycoord.EncodePartitionLoadInfoKey
	EncodePartitionLoadInfoPrefix = kvquerycoord.EncodePartitionLoadInfoPrefix
)
