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
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
)

// newTestCompactionTaskMeta builds a compactionTaskMeta backed by a permissive
// mock catalog. The compaction-leaf manager moved into
// pkg/v3/coordmeta/datacoord (taking its own test helper with it); this copy
// keeps the compaction-policy tests that stay in internal/datacoord working.
func newTestCompactionTaskMeta(t *testing.T) *compactionTaskMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	meta, _ := newCompactionTaskMeta(context.TODO(), catalog)
	return meta
}
