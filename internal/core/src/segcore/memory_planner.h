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
#pragma once

#include <vector>
#include <cstdint>
#include <cstddef>
#include "milvus-storage/common/metadata.h"

namespace milvus::segcore {

struct RowGroupBlock {
  int64_t offset;  // Start offset of the row group block
  int64_t count;   // Number of row groups in this block

  bool operator==(const RowGroupBlock& other) const { return offset == other.offset && count == other.count; }
};

const std::size_t MAX_ROW_GROUP_BLOCK_MEMORY = 16 << 20;

// Split row groups into blocks of appropriate size
std::vector<RowGroupBlock> split_row_groups(const std::vector<int64_t>& input_row_groups, const milvus_storage::RowGroupMetadataVector& row_group_metadatas);

}  // namespace milvus::segcore