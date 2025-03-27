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
#include <cstddef>
#include <algorithm>
#include "milvus-storage/common/metadata.h"
#include "segcore/memory_planner.h"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace milvus::segcore {

std::vector<RowGroupBlock> split_row_groups(const std::vector<int64_t>& input_row_groups,
                                          const milvus_storage::RowGroupMetadataVector& row_group_metadatas) {
    std::vector<RowGroupBlock> blocks;
    if (input_row_groups.empty()) {
        return blocks;
    }

    // Sort the row groups
    std::vector<int64_t> sorted_row_groups = input_row_groups;
    std::sort(sorted_row_groups.begin(), sorted_row_groups.end());

    int64_t current_start = sorted_row_groups[0];
    int64_t current_count = 1;
    int64_t current_memory = row_group_metadatas.Get(current_start).memory_size();

    for (size_t i = 1; i < sorted_row_groups.size(); ++i) {
        int64_t next_row_group = sorted_row_groups[i];
        int64_t next_memory = row_group_metadatas.Get(next_row_group).memory_size();

        if (next_row_group == current_start + current_count && 
            current_memory + next_memory <= MAX_ROW_GROUP_BLOCK_MEMORY) {
            current_count++;
            current_memory += next_memory;
            continue;
        }

        // If not continuous or exceeds memory limit, create a new block
        blocks.push_back({current_start, current_count});
        current_start = next_row_group;
        current_count = 1;
        current_memory = next_memory;
    }

    // Add the last block
    if (current_count > 0) {
        blocks.push_back({current_start, current_count});
    }

    return blocks;
}

}  // namespace milvus::segcore
