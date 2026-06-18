package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ---- Collection group ----

func (r *remoteMetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	resp, err := r.client.AddCollection(ctx, &catalogpb.AddCollectionRequest{Collection: model.MarshalCollectionModel(coll)})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DropCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	resp, err := r.client.DropCollection(ctx, &catalogpb.DropCollectionRequest{CollectionId: collectionID, Ts: ts})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	resp, err := r.client.RemoveCollection(ctx, &catalogpb.RemoveCollectionRequest{CollectionId: collectionID, Ts: ts})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) GetCollectionID(ctx context.Context, dbName string, collectionName string) UniqueID {
	resp, err := r.client.GetCollectionID(ctx, &catalogpb.GetCollectionIDRequest{DbName: dbName, CollectionName: collectionName})
	if err != nil {
		return InvalidCollectionID
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return InvalidCollectionID
	}
	return resp.GetCollectionId()
}

func (r *remoteMetaTable) GetCollectionByName(ctx context.Context, dbName string, collectionName string, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	resp, err := r.client.GetCollectionByName(ctx, &catalogpb.GetCollectionByNameRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Ts:               ts,
		AllowUnavailable: allowUnavailable,
	})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return model.UnmarshalCollectionModel(resp.GetCollection()), nil
}

func (r *remoteMetaTable) GetCollectionByID(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	resp, err := r.client.GetCollectionByID(ctx, &catalogpb.GetCollectionByIDRequest{
		DbName:           dbName,
		CollectionId:     collectionID,
		Ts:               ts,
		AllowUnavailable: allowUnavailable,
	})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return model.UnmarshalCollectionModel(resp.GetCollection()), nil
}

func (r *remoteMetaTable) GetCollectionByIDWithMaxTs(ctx context.Context, collectionID UniqueID) (*model.Collection, error) {
	resp, err := r.client.GetCollectionByIDWithMaxTs(ctx, &catalogpb.GetCollectionByIDWithMaxTsRequest{CollectionId: collectionID})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return model.UnmarshalCollectionModel(resp.GetCollection()), nil
}

func (r *remoteMetaTable) ListCollections(ctx context.Context, dbName string, ts Timestamp, onlyAvail bool) ([]*model.Collection, error) {
	resp, err := r.client.ListCollections(ctx, &catalogpb.ListCollectionsRequest{DbName: dbName, Ts: ts, OnlyAvail: onlyAvail})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	out := make([]*model.Collection, 0, len(resp.GetCollections()))
	for _, info := range resp.GetCollections() {
		out = append(out, model.UnmarshalCollectionModel(info))
	}
	return out, nil
}

func (r *remoteMetaTable) ListAllAvailCollections(ctx context.Context) map[int64][]int64 {
	resp, err := r.client.ListAllAvailCollections(ctx, &catalogpb.ListAllAvailCollectionsRequest{})
	if err != nil {
		return map[int64][]int64{}
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return map[int64][]int64{}
	}
	out := make(map[int64][]int64, len(resp.GetCollections()))
	for dbID, list := range resp.GetCollections() {
		out[dbID] = list.GetData()
	}
	return out
}

func (r *remoteMetaTable) ListAllAvailPartitions(ctx context.Context) map[int64]map[int64][]int64 {
	resp, err := r.client.ListAllAvailPartitions(ctx, &catalogpb.ListAllAvailPartitionsRequest{})
	if err != nil {
		return map[int64]map[int64][]int64{}
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return map[int64]map[int64][]int64{}
	}
	out := make(map[int64]map[int64][]int64, len(resp.GetDbPartitions()))
	for dbID, collParts := range resp.GetDbPartitions() {
		inner := make(map[int64][]int64, len(collParts.GetPartitions()))
		for collID, list := range collParts.GetPartitions() {
			inner[collID] = list.GetData()
		}
		out[dbID] = inner
	}
	return out
}

func (r *remoteMetaTable) ListCollectionPhysicalChannels(ctx context.Context) map[typeutil.UniqueID][]string {
	resp, err := r.client.ListCollectionPhysicalChannels(ctx, &catalogpb.ListCollectionPhysicalChannelsRequest{})
	if err != nil {
		return map[typeutil.UniqueID][]string{}
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return map[typeutil.UniqueID][]string{}
	}
	out := make(map[typeutil.UniqueID][]string, len(resp.GetChannels()))
	for collID, list := range resp.GetChannels() {
		out[collID] = list.GetData()
	}
	return out
}

func (r *remoteMetaTable) GetCollectionVirtualChannels(ctx context.Context, colID int64) []string {
	resp, err := r.client.GetCollectionVirtualChannels(ctx, &catalogpb.GetCollectionVirtualChannelsRequest{CollectionId: colID})
	if err != nil {
		return nil
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil
	}
	return resp.GetVirtualChannels()
}

func (r *remoteMetaTable) GetPChannelInfo(ctx context.Context, pchannel string) *rootcoordpb.GetPChannelInfoResponse {
	resp, err := r.client.GetPChannelInfo(ctx, &catalogpb.GetPChannelInfoRequest{Pchannel: pchannel})
	if err != nil {
		return &rootcoordpb.GetPChannelInfoResponse{Status: merr.Status(err)}
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return &rootcoordpb.GetPChannelInfoResponse{Status: merr.Status(err)}
	}
	return resp.GetInfo()
}

func (r *remoteMetaTable) CheckIfCollectionRenamable(ctx context.Context, dbName string, oldName string, newDBName string, newName string) error {
	resp, err := r.client.CheckIfCollectionRenamable(ctx, &catalogpb.CheckIfCollectionRenamableRequest{
		DbName:    dbName,
		OldName:   oldName,
		NewDbName: newDBName,
		NewName:   newName,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) BeginTruncateCollection(ctx context.Context, collectionID UniqueID) error {
	resp, err := r.client.BeginTruncateCollection(ctx, &catalogpb.BeginTruncateCollectionRequest{CollectionId: collectionID})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) GetGeneralCount(ctx context.Context) int {
	resp, err := r.client.GetGeneralCount(ctx, &catalogpb.GetGeneralCountRequest{})
	if err != nil {
		return 0
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return 0
	}
	return int(resp.GetCount())
}

// AlterCollection [B]: extract Header, MustBody and the max time tick from the
// BroadcastResult; the server rebuilds the result in memory and applies the meta change.
func (r *remoteMetaTable) AlterCollection(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	resp, err := r.client.AlterCollection(ctx, &catalogpb.AlterCollectionRequest{
		Header:   result.Message.Header(),
		Body:     result.Message.MustBody(),
		TimeTick: result.GetMaxTimeTick(),
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

// TruncateCollection [B]: extract Header and the per-vchannel time ticks (excluding the
// control channel) from the BroadcastResult; the server rebuilds Results faithfully so the
// meta apply can read result.Results[vchannel].TimeTick for each shard.
func (r *remoteMetaTable) TruncateCollection(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error {
	vchannelTimeTicks := make(map[string]uint64, len(result.Results))
	for vchannel, res := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		vchannelTimeTicks[vchannel] = res.TimeTick
	}
	resp, err := r.client.TruncateCollection(ctx, &catalogpb.TruncateCollectionRequest{
		Header:            result.Message.Header(),
		VchannelTimeTicks: vchannelTimeTicks,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
