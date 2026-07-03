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

// Package etcd now only re-exports the etcd-backed KV implementations that live
// in the shared pkg/v3/kv/etcd package, so the pooled catalog service (an
// independent module) can reuse the same single implementation. These aliases
// keep every existing internal/kv/etcd import site (server bootstraps, factory
// callers, migration tools, tests across querycoordv2/datacoord/rootcoord, ...)
// working unchanged.
package etcd

import (
	etcd "github.com/milvus-io/milvus/pkg/v3/kv/etcd"
)

// EmbedEtcdKV is the embedded-etcd MetaKv implementation.
type EmbedEtcdKV = etcd.EmbedEtcdKV

// Option configures an etcd KV client.
type Option = etcd.Option

// Re-exported constructors / factories / helpers.
var (
	NewEmbededEtcdKV               = etcd.NewEmbededEtcdKV
	NewEtcdKV                      = etcd.NewEtcdKV
	NewWatchKVFactory              = etcd.NewWatchKVFactory
	NewMetaKvFactory               = etcd.NewMetaKvFactory
	WithRequestTimeout             = etcd.WithRequestTimeout
	CheckElapseAndWarn             = etcd.CheckElapseAndWarn
	CheckValueSizeAndWarn          = etcd.CheckValueSizeAndWarn
	CheckTnxBytesValueSizeAndWarn  = etcd.CheckTnxBytesValueSizeAndWarn
	CheckTnxStringValueSizeAndWarn = etcd.CheckTnxStringValueSizeAndWarn
)
