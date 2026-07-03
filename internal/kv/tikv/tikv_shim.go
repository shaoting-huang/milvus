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

// Package tikv now only re-exports the TiKV-backed KV implementation that lives
// in the shared pkg/v3/kv/tikv package, so the pooled catalog service (an
// independent module) can construct a TiKV-backed RootCoordCatalog from the
// same single implementation. These aliases keep every existing
// internal/kv/tikv import site (root_coord.go, datacoord/server.go,
// querycoordv2/server.go, tsoutil, migration tools, ...) working unchanged.
package tikv

import (
	tikv "github.com/milvus-io/milvus/pkg/v3/kv/tikv"
)

// Exported constants re-exported from pkg/v3/kv/tikv.
const (
	MaxSnapshotTS    = tikv.MaxSnapshotTS
	EnableRollback   = tikv.EnableRollback
	EmptyValueString = tikv.EmptyValueString
)

// Exported package vars re-exported from pkg/v3/kv/tikv. None are mutated by
// external importers; they are surfaced here only to keep the original public
// surface identical.
var (
	Params           = tikv.Params
	SnapshotScanSize = tikv.SnapshotScanSize
	EmptyValueByte   = tikv.EmptyValueByte
)

// Option configures the TiKV KV client.
type Option = tikv.Option

// Re-exported constructors / helpers.
var (
	WithRequestTimeout = tikv.WithRequestTimeout
	NewTiKV            = tikv.NewTiKV
	CheckElapseAndWarn = tikv.CheckElapseAndWarn
)
