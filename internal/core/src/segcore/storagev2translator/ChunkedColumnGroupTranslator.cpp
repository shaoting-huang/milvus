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
#include "segcore/storagev2translator/ChunkedColumnGroupTranslator.h"
#include "common/GroupChunk.h"

#include "mmap/Types.h"
#include "common/Types.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/ThreadPools.h"
#include "segcore/Utils.h"

#include <string>
#include <vector>
#include <unordered_map>

#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"

namespace milvus::segcore::storagev2translator {

ChunkedColumnGroupTranslator::ChunkedColumnGroupTranslator(
    int64_t segment_id,
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    FieldDataInfo column_group_info,
    std::vector<std::string> insert_files,
    milvus::cachinglayer::StorageType storage_type,
    std::vector<milvus_storage::RowGroupMetadataVector>& row_group_meta_list,
    milvus_storage::FieldIDList field_id_list)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_cg_{}", segment_id, column_group_info.field_id)),
      field_metas_(field_metas),
      column_group_info_(column_group_info),
      insert_files_(insert_files),
      storage_type_(storage_type),
      row_group_meta_list_(row_group_meta_list),
      field_id_list_(field_id_list) {
    AssertInfo(insert_files_.size() == row_group_meta_list_.size(), 
              "Number of insert files must match number of row group metas");
}

size_t ChunkedColumnGroupTranslator::num_cells() const {
    size_t total_row_groups = 0;
    for (const auto& row_group_meta : row_group_meta_list_) {
        total_row_groups += row_group_meta.size();
    }
    return total_row_groups;
}

milvus::cachinglayer::cid_t 
ChunkedColumnGroupTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    size_t current_offset = 0;
    size_t total_row_groups = 0;
    
    for (size_t file_idx = 0; file_idx < row_group_meta_list_.size(); ++file_idx) {
        const auto& file_metas = row_group_meta_list_[file_idx];
        for (size_t rg_idx = 0; rg_idx < file_metas.size(); ++rg_idx) {
            const auto& meta = file_metas.Get(rg_idx);
            if (uid >= current_offset && uid < current_offset + meta.row_num()) {
                return total_row_groups + rg_idx;
            }
            current_offset += meta.row_num();
        }
        total_row_groups += file_metas.size();
    }
    return 0;
}

milvus::cachinglayer::StorageType 
ChunkedColumnGroupTranslator::storage_type() const {
    return storage_type_;
}

size_t ChunkedColumnGroupTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
    auto& row_group_meta = row_group_meta_list_[file_idx].Get(row_group_idx);
    return row_group_meta.memory_size();
}

const std::string& ChunkedColumnGroupTranslator::key() const {
    return key_;
}

std::pair<size_t, size_t>
ChunkedColumnGroupTranslator::get_file_and_row_group_index(milvus::cachinglayer::cid_t cid) const {
    size_t file_idx = 0;
    size_t remaining_cid = cid;
    
    for (; file_idx < row_group_meta_list_.size(); ++file_idx) {
        const auto& file_metas = row_group_meta_list_[file_idx];
        if (remaining_cid < file_metas.size()) {
            return {file_idx, remaining_cid};
        }
        remaining_cid -= file_metas.size();
    }
    
    return {0, 0}; // Default to first file and first row group if not found
}

std::vector<std::pair<cachinglayer::cid_t,
                     std::unique_ptr<milvus::GroupChunk>>>
ChunkedColumnGroupTranslator::get_cells(
    const std::vector<cachinglayer::cid_t>& cids) const {
    // Group cids by file
    std::vector<std::vector<int64_t>> file_row_groups(row_group_meta_list_.size());
    for (auto cid : cids) {
        auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
        file_row_groups[file_idx].push_back(row_group_idx);
    }
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
    pool.Submit(LoadArrowReaderFromStorageV2,
                insert_files_,
                column_group_info_.arrow_reader_channel,
                DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                parallel_degree,
                file_row_groups);

    LOG_INFO("segment {} submits load column group {} with fields {} task to thread pool",
             segment_id_,
             column_group_info_.field_id,
             field_id_list_.ToString());

    // Process the data based on storage type
    if (storage_type_ == cachinglayer::StorageType::MEMORY) {
        return load_column_group_in_memory(cids);
    } else {
        return load_column_group_in_mmap(cids);
    }
}

std::vector<std::pair<cachinglayer::cid_t,
                     std::unique_ptr<milvus::GroupChunk>>>
ChunkedColumnGroupTranslator::load_column_group_in_memory(
    const std::vector<cachinglayer::cid_t>& cids) const {
    
    std::vector<std::pair<cachinglayer::cid_t,
                         std::unique_ptr<milvus::GroupChunk>>> results;
    results.reserve(cids.size());
    
    std::shared_ptr<milvus::ArrowDataWrapper> r;
    size_t batch_idx = 0;
    while (column_group_info_.arrow_reader_channel->pop(r)) {
        for (const auto& batch : r->record_batches) {
            if (batch_idx >= cids.size()) {
                break;
            }
            auto cid = cids[batch_idx++];

            // Create chunks for each field in this batch
            std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
            
            // Iterate through field_id_list to get field_id and create chunk
            for (size_t i = 0; i < field_id_list_.size(); ++i) {
                auto field_id = field_id_list_.Get(i);
                auto it = field_metas_.find(milvus::FieldId(field_id));
                AssertInfo(it != field_metas_.end(), "Field id not found in field_metas");
                const auto& field_meta = it->second;
                
                auto dim = IsVectorDataType(field_meta.get_data_type()) &&
                          !IsSparseFloatVectorDataType(field_meta.get_data_type())
                          ? field_meta.get_dim()
                          : 1;
                arrow::ArrayVector array_vec;
                array_vec.push_back(batch->column(i));
                chunks[milvus::FieldId(field_id)] = milvus::create_chunk(field_meta, dim, array_vec);
            }
            
            // Create GroupChunk from chunks and store in results
            auto group_chunk = std::make_unique<milvus::GroupChunk>(std::move(chunks));
            results.emplace_back(cid, std::move(group_chunk));
        }
    }
    
    return results;
}

std::vector<std::pair<cachinglayer::cid_t,
                     std::unique_ptr<milvus::GroupChunk>>>
ChunkedColumnGroupTranslator::load_column_group_in_mmap(
    const std::vector<cachinglayer::cid_t>& cids) const {
    
    std::vector<std::pair<cachinglayer::cid_t,
                         std::unique_ptr<milvus::GroupChunk>>> results;
    results.reserve(cids.size());
    
    std::shared_ptr<milvus::ArrowDataWrapper> r;
    size_t batch_idx = 0;
    while (column_group_info_.arrow_reader_channel->pop(r)) {
        for (const auto& batch : r->record_batches) {
            if (batch_idx >= cids.size()) {
                break;
            }
            auto cid = cids[batch_idx++];

            // Create chunks for each field in this batch
            std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
            
            // Iterate through field_id_list to get field_id and create chunk
            for (size_t i = 0; i < field_id_list_.size(); ++i) {
                auto field_id = field_id_list_.Get(i);
                auto it = field_metas_.find(milvus::FieldId(field_id));
                AssertInfo(it != field_metas_.end(), "Field id not found in field_metas");
                const auto& field_meta = it->second;
                auto dim = IsVectorDataType(field_meta.get_data_type()) &&
                          !IsSparseFloatVectorDataType(field_meta.get_data_type())
                          ? field_meta.get_dim()
                          : 1;
                arrow::ArrayVector array_vec;
                array_vec.push_back(batch->column(i));
                chunks[milvus::FieldId(field_id)] = milvus::create_chunk(field_meta, dim, array_vec);
            }
            
            // Create GroupChunk from chunks and store in results
            auto group_chunk = std::make_unique<milvus::GroupChunk>(std::move(chunks));
            results.emplace_back(cid, std::move(group_chunk));
        }
    }
    
    return results;
}

}  // namespace milvus::segcore::storagev2translator