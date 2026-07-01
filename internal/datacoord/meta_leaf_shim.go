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

import coordmeta "github.com/milvus-io/milvus/pkg/v3/coordmeta/datacoord"

// The analyze and compaction leaf managers moved into the shared
// pkg/v3/coordmeta/datacoord package so the pooled catalog service can reuse the
// same implementations. These aliases keep the meta facade (which holds them as
// fields) and the ~dozen internal call sites compiling unchanged. The remaining
// leaf managers (index / stats-task / partition-stats / import / copy-segment)
// move in follow-ups.
type (
	analyzeMeta        = coordmeta.AnalyzeMeta
	compactionTaskMeta = coordmeta.CompactionTaskMeta
)

var (
	newAnalyzeMeta          = coordmeta.NewAnalyzeMeta
	newAnalyzeMetaWithTasks = coordmeta.NewAnalyzeMetaWithTasks
	newCompactionTaskMeta   = coordmeta.NewCompactionTaskMeta
)
