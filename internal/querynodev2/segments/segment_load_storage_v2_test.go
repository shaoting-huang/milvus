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

package segments

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/samber/lo"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	fPkOracle "github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type SegmentLoaderStorageV2Suite struct {
	suite.Suite
	loader Loader

	// Dependencies
	manager      *Manager
	rootPath     string
	chunkManager storage.ChunkManager

	// Data
	collectionID int64
	partitionID  int64
	segmentID    int64
	schema       *schemapb.CollectionSchema
	segmentNum   int

	meta *etcdpb.CollectionMeta
}

func (s *SegmentLoaderStorageV2Suite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collectionID = testutil.CollectionID
	s.partitionID = testutil.PartitionID
	s.segmentID = testutil.SegmentID
	s.meta = testutil.GenTestCollectionMeta()
	s.schema = s.meta.Schema
}

func (s *SegmentLoaderStorageV2Suite) SetupTest() {
	ctx := context.Background()
	s.meta = testutil.GenTestCollectionMeta()
	s.rootPath = paramtable.Get().LocalStorageCfg.Path.GetValue()
	paramtable.Get().Save("common.storageType", "local")
	paramtable.Get().Save("common.storage.enableV2", "true")
	initcore.InitStorageV2FileSystem(paramtable.Get())

	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), s.rootPath)
	s.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)

	// Dependencies
	s.manager = NewManager()
	indexMeta := mock_segcore.GenTestIndexMeta(s.collectionID, s.schema)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
	}
	s.manager.Collection.PutOrRef(s.collectionID, s.schema, indexMeta, loadMeta)
	s.loader = NewLoader(ctx, s.manager, s.chunkManager)
	initcore.InitRemoteChunkManager(paramtable.Get())
}

func (s *SegmentLoaderStorageV2Suite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
	paramtable.Get().Reset("common.storageType")
	paramtable.Get().Reset("common.storage.enableV2")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "insert_log")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "delta_log")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "stats_log")
}

func (s *SegmentLoaderStorageV2Suite) TestLoad() {
	ctx := context.Background()

	msgLength := 4

	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)
	binlogs, _, statsLogs, _, _, err := s.initStorageV2Segments(1, testutil.SegmentID, alloc)

	s.NoError(err)
	info := &querypb.SegmentLoadInfo{
		SegmentID:      testutil.SegmentID,
		PartitionID:    testutil.PartitionID,
		CollectionID:   testutil.CollectionID,
		BinlogPaths:    lo.Values(binlogs),
		Statslogs:      lo.Values(statsLogs),
		NumOfRows:      int64(msgLength),
		InsertChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", testutil.CollectionID),
		StorageVersion: storage.StorageV2,
	}
	_, err = s.loader.Load(ctx, testutil.CollectionID, SegmentTypeSealed, 0, info)
	s.NoError(err)

	// // Load growing
	// binlogs, statsLogs, err = mock_segcore.SaveBinLog(ctx,
	// 	suite.collectionID,
	// 	suite.partitionID,
	// 	suite.segmentID+1,
	// 	msgLength,
	// 	suite.schema,
	// 	suite.chunkManager,
	// )
	// suite.NoError(err)

	// _, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
	// 	SegmentID:     suite.segmentID + 1,
	// 	PartitionID:   suite.partitionID,
	// 	CollectionID:  suite.collectionID,
	// 	BinlogPaths:   binlogs,
	// 	Statslogs:     statsLogs,
	// 	NumOfRows:     int64(msgLength),
	// 	InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	// })
	// suite.NoError(err)
}

func (s *SegmentLoaderStorageV2Suite) initStorageV2Segments(rows int, seed int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	size int64,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := fPkOracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(s.collectionID).Maybe()
	mc.EXPECT().Schema().Return(s.meta.Schema).Maybe()
	mc.EXPECT().GetSegmentByID(seed).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID)
	pack := new(syncmgr.SyncPack).WithCollectionID(s.collectionID).WithPartitionID(s.partitionID).WithSegmentID(seed).WithChannelName(channelName).WithInsertData(testutil.GetInsertData(rows, seed, s.meta.GetSchema()))
	bw := syncmgr.NewBulkPackWriterV2(mc, cm, alloc, packed.DefaultWriteBufferSize, 0)
	return bw.Write(context.Background(), pack)
}

func TestSegmentLoaderStorageV2(t *testing.T) {
	suite.Run(t, &SegmentLoaderStorageV2Suite{})
}
