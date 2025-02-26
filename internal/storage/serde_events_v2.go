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

package storage

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type ChunkedPathsReader func() ([]string, error)

type packedRecordReader struct {
	pathsReader ChunkedPathsReader
	reader      *packed.PackedReader

	bufferSize  int64
	arrowSchema *arrow.Schema
	field2Col   map[FieldID]int
}

var _ RecordReader = (*packedRecordReader)(nil)

func (pr *packedRecordReader) iterateNextBatch() error {
	if pr.reader != nil {
		pr.reader.Close()
	}

	paths, err := pr.pathsReader()
	if err != nil {
		return err
	}

	reader, err := packed.NewPackedReader(paths, pr.arrowSchema, pr.bufferSize)
	if err != nil {
		return merr.WrapErrParameterInvalid("New binlog record packed reader error: %s", err.Error())
	}
	pr.reader = reader
	return nil
}

func (pr *packedRecordReader) Next() (Record, error) {
	if pr.reader == nil {
		if err := pr.iterateNextBatch(); err != nil {
			return nil, err
		}
	}

	for {
		rec, err := pr.reader.ReadNext()
		if err != nil {
			return nil, err
		}
		if rec != nil {
			return NewSimpleArrowRecord(rec, pr.field2Col), nil
		}
		if err := pr.iterateNextBatch(); err != nil {
			return nil, err
		}
	}
}

func (pr *packedRecordReader) Close() error {
	if pr.reader != nil {
		pr.reader.Close()
	}
	return nil
}

func newPackedRecordReader(pathsReader ChunkedPathsReader, schema *schemapb.CollectionSchema, bufferSize int64,
) (*packedRecordReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	field2Col := make(map[FieldID]int)
	for i, field := range schema.Fields {
		field2Col[field.FieldID] = i
	}
	return &packedRecordReader{
		pathsReader: pathsReader,
		bufferSize:  bufferSize,
		arrowSchema: arrowSchema,
		field2Col:   field2Col,
	}, nil
}

func NewPackedDeserializeReader(pathsReader ChunkedPathsReader, schema *schemapb.CollectionSchema,
	bufferSize int64,
) (*DeserializeReader[*Value], error) {
	reader, err := newPackedRecordReader(pathsReader, schema, bufferSize)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		pkField := func() *schemapb.FieldSchema {
			for _, field := range schema.Fields {
				if field.GetIsPrimaryKey() {
					return field
				}
			}
			return nil
		}()
		if pkField == nil {
			return merr.WrapErrServiceInternal("no primary key field found")
		}

		rec, ok := r.(*simpleArrowRecord)
		if !ok {
			return merr.WrapErrServiceInternal("can not cast to simple arrow record")
		}

		numFields := len(schema.Fields)
		for i := 0; i < rec.Len(); i++ {
			if v[i] == nil {
				v[i] = &Value{
					Value: make(map[FieldID]interface{}, numFields),
				}
			}
			value := v[i]
			m := value.Value.(map[FieldID]interface{})
			for _, field := range schema.Fields {
				fieldID := field.FieldID
				column := r.Column(fieldID)
				if column.IsNull(i) {
					m[fieldID] = nil
				} else {
					d, ok := serdeMap[field.DataType].deserialize(column, i)
					if ok {
						m[fieldID] = d
					} else {
						return merr.WrapErrServiceInternal(fmt.Sprintf("can not deserialize field [%s]", field.Name))
					}
				}
			}

			rowID, ok := m[common.RowIDField].(int64)
			if !ok {
				return merr.WrapErrIoKeyNotFound("no row id column found")
			}
			value.ID = rowID
			value.Timestamp = m[common.TimeStampField].(int64)

			pkCol := rec.field2Col[pkField.FieldID]
			pk, err := GenPrimaryKeyByRawData(m[pkField.FieldID], schema.Fields[pkCol].DataType)
			if err != nil {
				return err
			}

			value.PK = pk
			value.IsDeleted = false
			value.Value = m
		}
		return nil
	}), nil
}

var _ RecordWriter = (*packedRecordWriter)(nil)

type packedRecordWriter struct {
	writer *packed.PackedWriter

	bufferSize          int64
	multiPartUploadSize int64
	columnGroups        [][]int
	paths               []string
	schema              *arrow.Schema

	numRows             int
	writtenUncompressed uint64
}

func (pw *packedRecordWriter) Write(r Record) error {
	rec, ok := r.(*simpleArrowRecord)
	if !ok {
		return merr.WrapErrServiceInternal("can not cast to simple arrow record")
	}
	pw.numRows += r.Len()
	for _, arr := range rec.r.Columns() {
		pw.writtenUncompressed += uint64(calculateArraySize(arr))
	}
	defer rec.Release()
	return pw.writer.WriteRecordBatch(rec.r)
}

func (pw *packedRecordWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *packedRecordWriter) Close() error {
	if pw.writer != nil {
		return pw.writer.Close()
	}
	return nil
}

func NewPackedRecordWriter(paths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups [][]int) (*packedRecordWriter, error) {
	writer, err := packed.NewPackedWriter(paths, schema, bufferSize, multiPartUploadSize, columnGroups)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	return &packedRecordWriter{
		writer:     writer,
		schema:     schema,
		bufferSize: bufferSize,
		paths:      paths,
	}, nil
}

func NewPackedSerializeWriter(paths []string, schema *schemapb.CollectionSchema, bufferSize int64, multiPartUploadSize int64, columnGroups [][]int, batchSize int) (*SerializeWriter[*Value], error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	packedRecordWriter, err := NewPackedRecordWriter(paths, arrowSchema, bufferSize, multiPartUploadSize, columnGroups)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	return NewSerializeRecordWriter[*Value](packedRecordWriter, func(v []*Value) (Record, error) {
		return ValueSerializer(v, schema.Fields)
	}, batchSize), nil
}
