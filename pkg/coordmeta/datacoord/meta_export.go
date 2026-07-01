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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// The helpers below exist so that datacoord task / inspector tests that stay in
// internal/datacoord (and therefore cannot reach the unexported fields of the
// moved leaf managers) can inject and mutate state in place of the struct
// literals / direct field writes they used before the move.

// NewAnalyzeMetaWithTasks builds an AnalyzeMeta backed by catalog with the given
// pre-seeded task map, WITHOUT reloading from the catalog.
func NewAnalyzeMetaWithTasks(ctx context.Context, catalog metastore.DataCoordCatalog, tasks map[int64]*indexpb.AnalyzeTask) *AnalyzeMeta {
	if tasks == nil {
		tasks = make(map[int64]*indexpb.AnalyzeTask)
	}
	return &AnalyzeMeta{
		ctx:     ctx,
		catalog: catalog,
		tasks:   tasks,
	}
}

// SetCatalog re-points the AnalyzeMeta at a different catalog. Test-only seam.
func (m *AnalyzeMeta) SetCatalog(catalog metastore.DataCoordCatalog) {
	m.catalog = catalog
}

// Tasks returns the live taskID -> AnalyzeTask map. Test-only seam.
func (m *AnalyzeMeta) Tasks() map[int64]*indexpb.AnalyzeTask {
	return m.tasks
}

// SetCatalog re-points the CompactionTaskMeta at a different catalog. Test-only seam.
func (csm *CompactionTaskMeta) SetCatalog(catalog metastore.DataCoordCatalog) {
	csm.catalog = catalog
}
