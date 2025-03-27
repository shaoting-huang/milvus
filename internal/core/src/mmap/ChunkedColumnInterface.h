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

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/CacheSlot.h"
#include "common/Chunk.h"

namespace milvus {

class ChunkedColumnInterface {
 public:
    virtual ~ChunkedColumnInterface() = default;

    // Get raw data pointer of a specific chunk
    virtual cachinglayer::PinWrapper<const char*>
    DataOfChunk(int chunk_id) const = 0;

    // Check if the value at given offset is valid (not null)
    virtual bool
    IsValid(size_t offset) const = 0;

    // Check if the column can contain null values
    virtual bool
    IsNullable() const = 0;

    // Get total number of rows in the column
    virtual size_t
    NumRows() const = 0;

    // Get total number of chunks in the column
    virtual int64_t
    num_chunks() const = 0;

    // Get total byte size of the column data
    virtual size_t
    DataByteSize() const = 0;

    // Get number of rows in a specific chunk
    virtual int64_t
    chunk_row_nums(int64_t chunk_id) const = 0;

    // Convert a global offset to (chunk_id, offset_in_chunk) pair
    virtual std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const = 0;

    // Get number of rows before a specific chunk
    virtual int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const = 0;

    // Get vector of row counts before each chunk
    virtual const std::vector<int64_t>&
    GetNumRowsUntilChunk() const = 0;

    virtual cachinglayer::PinWrapper<Chunk*>
    GetChunk(int64_t chunk_id) = 0;
};

}  // namespace milvus 