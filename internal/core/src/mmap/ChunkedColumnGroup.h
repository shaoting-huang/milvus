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

#include <folly/io/IOBuf.h>
#include <sys/mman.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>
#include <math.h>

#include "common/Array.h"
#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/Span.h"
#include "common/Array.h"
#include "mmap/ChunkedColumn.h"

namespace milvus {

using GroupChunkVector = std::vector<std::shared_ptr<GroupChunk>>;

// ChunkedColumnGroup represents a collection of group chunks
class ChunkedColumnGroup {
 public:
    ChunkedColumnGroup() = default;

    ChunkedColumnGroup(const std::vector<std::shared_ptr<GroupChunk>>& group_chunk_vector) {
        for (const auto& group_chunk : group_chunk_vector) {
            AddGroupChunk(group_chunk);
        }
    }

    // Add a group chunk to the group
    void AddGroupChunk(std::shared_ptr<GroupChunk> multi_column_chunk) {
        group_chunk_vector.push_back(std::move(multi_column_chunk));
    }

    // Get the number of group chunks
    size_t NumGroupChunks() const {
        return group_chunk_vector.size();
    }

    // Get a specific group chunk by index
    std::shared_ptr<GroupChunk> GetGroupChunk(size_t index) const {
        if (index >= group_chunk_vector.size()) {
            return nullptr;
        }
        return group_chunk_vector[index];
    }

    // Get all group chunks
    const GroupChunkVector& GetGroupChunkVector() const {
        return group_chunk_vector;
    }

    // Get the total number of rows across all group chunks
    int64_t NumRows() const {
        int64_t total_rows = 0;
        for (const auto& group_chunk : group_chunk_vector) {
            total_rows += group_chunk->RowNums();
        }
        return total_rows;
    }

    size_t DataByteSize() const {
        size_t total_size = 0;
        for (const auto& group_chunk : group_chunk_vector) {
            total_size += group_chunk->Size();
        }
        return total_size;
    }

    // Get the number of rows in a specific group chunk
    int64_t GetGroupChunkRowNums(size_t index) const {
        if (index >= group_chunk_vector.size()) {
            return 0;
        }
        return group_chunk_vector[index]->RowNums();
    }

    // Get the chunk for a specific column in a specific group chunk
    std::shared_ptr<Chunk> GetColumnChunk(size_t multi_column_chunk_index, FieldId field_id) const {
        if (multi_column_chunk_index >= group_chunk_vector.size()) {
            return nullptr;
        }
        return group_chunk_vector[multi_column_chunk_index]->GetChunk(field_id);
    }

 private:
    GroupChunkVector group_chunk_vector;
};

// ProxyChunkColumn is used to access a specific column from a ChunkedColumnGroup
class ProxyChunkColumn : public ChunkedColumnBase {
 public:
    explicit ProxyChunkColumn(std::shared_ptr<ChunkedColumnGroup> group,
                            FieldId field_id,
                            const FieldMeta& field_meta)
        : group_(group), field_id_(field_id), field_meta_(field_meta) {
    }

    ~ProxyChunkColumn() override = default;

    const char*
    Data(int chunk_id) const override {
        return GetTargetColumn(chunk_id)->Data(chunk_id);
    }

    const char*
    ValueAt(int64_t offset) const override {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        return GetTargetColumn(chunk_id)->ValueAt(offset_in_chunk);
    }

    // TODO: check with chunk cache
    const char*
    MmappedData() const override {
        return GetTargetColumn(0)->MmappedData();
    }

    bool
    IsValid(size_t offset) const override {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        return GetTargetColumn(chunk_id)->IsValid(offset_in_chunk);
    }

    bool
    IsValid(int64_t chunk_id, int64_t offset) const override {
        return GetTargetColumn(chunk_id)->IsValid(chunk_id, offset);
    }

    bool
    IsNullable() const override {
        return GetTargetColumn(0)->IsNullable();
    }

    size_t
    NumRows() const override {
        return group_->NumRows();
    }

    int64_t
    num_chunks() const override {
        return group_->NumGroupChunks();
    }

    void
    AddChunk(std::shared_ptr<Chunk> chunk) override {
        PanicInfo(ErrorCode::Unsupported, "AddChunk not supported for ProxyChunkColumn");
    }

    size_t
    DataByteSize() const override {
        size_t total_size = 0;
        for (int64_t i = 0; i < num_chunks(); ++i) {
            total_size += GetTargetColumn(i)->DataByteSize();
        }
        return total_size;
    }

    SpanBase
    Span(int64_t chunk_id) const override {
        return GetTargetColumn(chunk_id)->Span(chunk_id);
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    StringViews(int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        return GetTargetColumn(chunk_id)->StringViews(chunk_id, offset_len);
    }

    std::pair<std::vector<ArrayView>, FixedVector<bool>>
    ArrayViews(int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        return GetTargetColumn(chunk_id)->ArrayViews(chunk_id, offset_len);
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    ViewsByOffsets(int64_t chunk_id,
                   const FixedVector<int32_t>& offsets) const override {
        return GetTargetColumn(chunk_id)->ViewsByOffsets(chunk_id, offsets);
    }

    BufferView
    GetBatchBuffer(int64_t chunk_id,
                   int64_t start_offset,
                   int64_t length) override {
        return GetTargetColumn(chunk_id)->GetBatchBuffer(chunk_id, start_offset, length);
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        return group_->GetGroupChunkRowNums(chunk_id);
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        int64_t total_rows = 0;
        for (int64_t i = 0; i < num_chunks(); ++i) {
            int64_t chunk_rows = chunk_row_nums(i);
            if (offset < total_rows + chunk_rows) {
                return {i, offset - total_rows};
            }
            total_rows += chunk_rows;
        }
        PanicInfo(ErrorCode::OutOfRange, "offset {} is out of range", offset);
    }

    std::shared_ptr<Chunk>
    GetChunk(int64_t chunk_id) const override {
        return group_->GetColumnChunk(chunk_id, field_id_);
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        int64_t total_rows = 0;
        for (int64_t i = 0; i < chunk_id; ++i) {
            total_rows += chunk_row_nums(i);
        }
        return total_rows;
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        static std::vector<int64_t> rows_until_chunk;
        rows_until_chunk.clear();
        int64_t total_rows = 0;
        for (int64_t i = 0; i < num_chunks(); ++i) {
            rows_until_chunk.push_back(total_rows);
            total_rows += chunk_row_nums(i);
        }
        return rows_until_chunk;
    }

 private:
    std::shared_ptr<ChunkedColumnBase>
    GetTargetColumn(int64_t chunk_id) const {
        AssertInfo(group_ != nullptr, "Column group is null");
        std::vector<std::shared_ptr<Chunk>> chunks;
        for (int i = 0; i < group_->NumGroupChunks(); ++i) {
            auto chunk = group_->GetColumnChunk(chunk_id, field_id_);
            AssertInfo(chunk != nullptr, "Column chunk not found for field_id {} in chunk {}", 
                     field_id_.get(), chunk_id);
            chunks.push_back(std::move(chunk));
        }
        return std::make_shared<ChunkedColumn>(field_meta_, chunks);
    }

    std::shared_ptr<ChunkedColumnGroup> group_;
    FieldId field_id_;
    FieldMeta field_meta_;
};

}  // namespace milvus