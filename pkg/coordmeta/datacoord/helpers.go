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

import "github.com/milvus-io/milvus/pkg/v3/proto/datapb"

// isCompactionTaskFinished mirrors the helper of the same name in
// internal/datacoord/compaction_util.go. compactionTaskMeta moved into this
// shared package and needs it, but compaction_util.go (and its many other
// helpers) stays in internal/datacoord, so the leaf copy lives here. Keep the
// two in sync until the rest of the compaction code follows into pkg.
func isCompactionTaskFinished(t *datapb.CompactionTask) bool {
	switch t.GetState() {
	case datapb.CompactionTaskState_timeout,
		datapb.CompactionTaskState_completed,
		datapb.CompactionTaskState_cleaned,
		datapb.CompactionTaskState_unknown:
		return true
	default:
		return false
	}
}
