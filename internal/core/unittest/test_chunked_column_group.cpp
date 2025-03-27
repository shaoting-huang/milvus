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

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "common/ChunkWriter.h"
#include "mmap/ChunkedColumnGroup.h"

using namespace milvus;

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta, int64_t dim, const FixedVector<int64_t>& data) {
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::INT64);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    event_data.field_data = field_data;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    return milvus::create_chunk(field_meta, dim, array_vec);
}

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta, int64_t dim, const FixedVector<std::string>& data) {
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::VARCHAR);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    event_data.field_data = field_data;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    return milvus::create_chunk(field_meta, dim, array_vec);
}

TEST(group_chunk, basic) {
    FixedVector<int64_t> int64_data = {1, 2, 3, 4, 5};
    FixedVector<std::string> string_data = {"a", "b", "c", "d", "e"};
    FieldMeta int64_field_meta(FieldName("int64_field"),
                              milvus::FieldId(1),
                              DataType::INT64,
                              false,
                              std::nullopt);
    FieldMeta string_field_meta(FieldName("string_field"),
                               milvus::FieldId(2),
                               DataType::STRING,
                               false,
                               std::nullopt);
    auto int64_chunk = create_chunk(int64_field_meta, 1, int64_data);
    auto string_chunk = create_chunk(string_field_meta, 1, string_data);

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_shared<GroupChunk>(chunks);

    EXPECT_EQ(group_chunk->RowNums(), 5);
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(1)));
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(2)));
    EXPECT_FALSE(group_chunk->HasChunk(FieldId(3)));

    auto retrieved_int64_chunk = group_chunk->GetChunk(FieldId(1));
    auto retrieved_string_chunk = group_chunk->GetChunk(FieldId(2));
    EXPECT_NE(retrieved_int64_chunk, nullptr);
    EXPECT_NE(retrieved_string_chunk, nullptr);
    EXPECT_EQ(retrieved_int64_chunk->RowNums(), 5);
    EXPECT_EQ(retrieved_string_chunk->RowNums(), 5);

    auto all_chunks = group_chunk->GetChunks();
    EXPECT_EQ(all_chunks.size(), 2);
    EXPECT_EQ(all_chunks[FieldId(1)], int64_chunk);
    EXPECT_EQ(all_chunks[FieldId(2)], string_chunk);
}

TEST(group_chunk, add_chunk) {
    FixedVector<int64_t> int64_data = {1, 2, 3, 4, 5};
    FixedVector<std::string> string_data = {"a", "b", "c", "d", "e"};

    FieldMeta int64_field_meta(FieldName("int64_field"),
                              milvus::FieldId(1),
                              DataType::INT64,
                              false,
                              std::nullopt);
    FieldMeta string_field_meta(FieldName("string_field"),
                               milvus::FieldId(2),
                               DataType::STRING,
                               false,
                               std::nullopt);
    auto int64_chunk = create_chunk(int64_field_meta, 1, int64_data);
    auto string_chunk = create_chunk(string_field_meta, 1, string_data);


    auto group_chunk = std::make_shared<GroupChunk>();
    group_chunk->AddChunk(FieldId(1), int64_chunk);
    EXPECT_EQ(group_chunk->RowNums(), 5);
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(1)));
    EXPECT_FALSE(group_chunk->HasChunk(FieldId(2)));

    group_chunk->AddChunk(FieldId(2), string_chunk);
    EXPECT_EQ(group_chunk->RowNums(), 5);
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(1)));
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(2)));

    auto retrieved_int64_chunk = group_chunk->GetChunk(FieldId(1));
    auto retrieved_string_chunk = group_chunk->GetChunk(FieldId(2));
    EXPECT_NE(retrieved_int64_chunk, nullptr);
    EXPECT_NE(retrieved_string_chunk, nullptr);
    EXPECT_EQ(retrieved_int64_chunk->RowNums(), 5);
    EXPECT_EQ(retrieved_string_chunk->RowNums(), 5);
}

TEST(group_chunk, empty_chunk) {
    auto group_chunk = std::make_shared<GroupChunk>();

    EXPECT_EQ(group_chunk->RowNums(), 0);
    EXPECT_FALSE(group_chunk->HasChunk(FieldId(1)));
    EXPECT_EQ(group_chunk->GetChunk(FieldId(1)), nullptr);
    EXPECT_TRUE(group_chunk->GetChunks().empty());
}

TEST(chunked_column_group, basic) {
    FixedVector<int64_t> int64_data = {1, 2, 3, 4, 5};
    FixedVector<std::string> string_data = {"a", "b", "c", "d", "e"};

    FieldMeta int64_field_meta(FieldName("int64_field"),
                              milvus::FieldId(1),
                              DataType::INT64,
                              false,
                              std::nullopt);
    FieldMeta string_field_meta(FieldName("string_field"),
                               milvus::FieldId(2),
                               DataType::STRING,
                               false,
                               std::nullopt);
    auto int64_chunk = create_chunk(int64_field_meta, 1, int64_data);
    auto string_chunk = create_chunk(string_field_meta, 1, string_data);


    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_shared<GroupChunk>(chunks);

    auto column_group = std::make_shared<ChunkedColumnGroup>();
    column_group->AddGroupChunk(group_chunk);

    EXPECT_EQ(column_group->NumGroupChunks(), 1);
    EXPECT_EQ(column_group->NumRows(), 5);
    EXPECT_EQ(column_group->GetGroupChunkRowNums(0), 5);
    EXPECT_EQ(column_group->GetGroupChunkRowNums(1), 0);  // Out of range

    auto retrieved_group_chunk = column_group->GetGroupChunk(0);
    EXPECT_NE(retrieved_group_chunk, nullptr);
    EXPECT_EQ(retrieved_group_chunk->RowNums(), 5);
    EXPECT_EQ(column_group->GetGroupChunk(1), nullptr);  // Out of range

    auto retrieved_int64_chunk = column_group->GetColumnChunk(0, FieldId(1));
    auto retrieved_string_chunk = column_group->GetColumnChunk(0, FieldId(2));
    EXPECT_NE(retrieved_int64_chunk, nullptr);
    EXPECT_NE(retrieved_string_chunk, nullptr);
    EXPECT_EQ(retrieved_int64_chunk->RowNums(), 5);
    EXPECT_EQ(retrieved_string_chunk->RowNums(), 5);
    EXPECT_EQ(column_group->GetColumnChunk(1, FieldId(1)), nullptr);  // Out of range
    EXPECT_EQ(column_group->GetColumnChunk(0, FieldId(3)), nullptr);  // Non-existent field

    const auto& group_chunk_vector = column_group->GetGroupChunkVector();
    EXPECT_EQ(group_chunk_vector.size(), 1);
    EXPECT_EQ(group_chunk_vector[0], group_chunk);
}

TEST(chunked_column_group, multiple_group_chunks) {
    FixedVector<int64_t> int64_data1 = {1, 2, 3};
    FixedVector<std::string> string_data1 = {"a", "b", "c"};
    FixedVector<int64_t> int64_data2 = {4, 5};
    FixedVector<std::string> string_data2 = {"d", "e"};
    FieldMeta int64_field_meta(FieldName("int64_field"),
                              milvus::FieldId(1),
                              DataType::INT64,
                              false,
                              std::nullopt);
    FieldMeta string_field_meta(FieldName("string_field"),
                               milvus::FieldId(2),
                               DataType::STRING,
                               false,
                               std::nullopt);
    auto int64_chunk1 = create_chunk(int64_field_meta, 1, int64_data1);
    auto string_chunk1 = create_chunk(string_field_meta, 1, string_data1);
    auto int64_chunk2 = create_chunk(int64_field_meta, 1, int64_data2);
    auto string_chunk2 = create_chunk(string_field_meta, 1, string_data2);

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks1;
    chunks1[FieldId(1)] = int64_chunk1;
    chunks1[FieldId(2)] = string_chunk1;
    auto group_chunk1 = std::make_shared<GroupChunk>(chunks1);

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks2;
    chunks2[FieldId(1)] = int64_chunk2;
    chunks2[FieldId(2)] = string_chunk2;
    auto group_chunk2 = std::make_shared<GroupChunk>(chunks2);

    auto column_group = std::make_shared<ChunkedColumnGroup>();
    column_group->AddGroupChunk(group_chunk1);
    column_group->AddGroupChunk(group_chunk2);

    EXPECT_EQ(column_group->NumGroupChunks(), 2);
    EXPECT_EQ(column_group->NumRows(), 5);  // 3 + 2 rows
    EXPECT_EQ(column_group->GetGroupChunkRowNums(0), 3);
    EXPECT_EQ(column_group->GetGroupChunkRowNums(1), 2);

    auto retrieved_group_chunk1 = column_group->GetGroupChunk(0);
    auto retrieved_group_chunk2 = column_group->GetGroupChunk(1);
    EXPECT_NE(retrieved_group_chunk1, nullptr);
    EXPECT_NE(retrieved_group_chunk2, nullptr);
    EXPECT_EQ(retrieved_group_chunk1->RowNums(), 3);
    EXPECT_EQ(retrieved_group_chunk2->RowNums(), 2);

    auto retrieved_int64_chunk1 = column_group->GetColumnChunk(0, FieldId(1));
    auto retrieved_string_chunk1 = column_group->GetColumnChunk(0, FieldId(2));
    auto retrieved_int64_chunk2 = column_group->GetColumnChunk(1, FieldId(1));
    auto retrieved_string_chunk2 = column_group->GetColumnChunk(1, FieldId(2));
    EXPECT_NE(retrieved_int64_chunk1, nullptr);
    EXPECT_NE(retrieved_string_chunk1, nullptr);
    EXPECT_NE(retrieved_int64_chunk2, nullptr);
    EXPECT_NE(retrieved_string_chunk2, nullptr);
    EXPECT_EQ(retrieved_int64_chunk1->RowNums(), 3);
    EXPECT_EQ(retrieved_string_chunk1->RowNums(), 3);
    EXPECT_EQ(retrieved_int64_chunk2->RowNums(), 2);
    EXPECT_EQ(retrieved_string_chunk2->RowNums(), 2);

    const auto& group_chunk_vector = column_group->GetGroupChunkVector();
    EXPECT_EQ(group_chunk_vector.size(), 2);
    EXPECT_EQ(group_chunk_vector[0], group_chunk1);
    EXPECT_EQ(group_chunk_vector[1], group_chunk2);
}

TEST(chunked_column_group, proxy_column) {
    FixedVector<int64_t> int64_data = {1, 2, 3, 4, 5};
    FixedVector<std::string> string_data = {"a", "b", "c", "d", "e"};
    FieldMeta int64_field_meta(FieldName("int64_field"),
                              milvus::FieldId(1),
                              DataType::INT64,
                              false,
                              std::nullopt);
    FieldMeta string_field_meta(FieldName("string_field"),
                               milvus::FieldId(2),
                               DataType::STRING,
                               false,
                               std::nullopt);
    auto int64_chunk = create_chunk(int64_field_meta, 1, int64_data);
    auto string_chunk = create_chunk(string_field_meta, 1, string_data);

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_shared<GroupChunk>(chunks);

    auto column_group = std::make_shared<ChunkedColumnGroup>();
    column_group->AddGroupChunk(group_chunk);

    auto proxy_int64 = std::make_shared<ProxyChunkColumn>(column_group, FieldId(1), int64_field_meta);
    EXPECT_EQ(proxy_int64->NumRows(), 5);
    EXPECT_EQ(proxy_int64->num_chunks(), 1);
    EXPECT_FALSE(proxy_int64->IsNullable());
    auto data = proxy_int64->Data(0);
    EXPECT_NE(data, nullptr);
    auto value = proxy_int64->ValueAt(0);
    EXPECT_NE(value, nullptr);
    EXPECT_TRUE(proxy_int64->IsValid(0));
    EXPECT_TRUE(proxy_int64->IsValid(0, 0));
    EXPECT_THROW(proxy_int64->AddChunk(int64_chunk), milvus::SegcoreError);

    auto proxy_string = std::make_shared<ProxyChunkColumn>(column_group, FieldId(2), string_field_meta);
    EXPECT_EQ(proxy_string->NumRows(), 5);
    EXPECT_EQ(proxy_string->num_chunks(), 1);
    EXPECT_FALSE(proxy_string->IsNullable());
} 