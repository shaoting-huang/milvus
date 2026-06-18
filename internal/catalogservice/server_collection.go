package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Collection group ----

func (s *Server) AddCollection(ctx context.Context, req *catalogpb.AddCollectionRequest) (*catalogpb.AddCollectionResponse, error) {
	err := s.meta.AddCollection(ctx, model.UnmarshalCollectionModel(req.GetCollection()))
	return &catalogpb.AddCollectionResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DropCollection(ctx context.Context, req *catalogpb.DropCollectionRequest) (*catalogpb.DropCollectionResponse, error) {
	err := s.meta.DropCollection(ctx, req.GetCollectionId(), req.GetTs())
	return &catalogpb.DropCollectionResponse{Status: merr.Status(err)}, nil
}

func (s *Server) RemoveCollection(ctx context.Context, req *catalogpb.RemoveCollectionRequest) (*catalogpb.RemoveCollectionResponse, error) {
	err := s.meta.RemoveCollection(ctx, req.GetCollectionId(), req.GetTs())
	return &catalogpb.RemoveCollectionResponse{Status: merr.Status(err)}, nil
}

func (s *Server) GetCollectionID(ctx context.Context, req *catalogpb.GetCollectionIDRequest) (*catalogpb.GetCollectionIDResponse, error) {
	id := s.meta.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	return &catalogpb.GetCollectionIDResponse{Status: merr.Success(), CollectionId: id}, nil
}

func (s *Server) GetCollectionByName(ctx context.Context, req *catalogpb.GetCollectionByNameRequest) (*catalogpb.GetCollectionByNameResponse, error) {
	coll, err := s.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), req.GetTs(), req.GetAllowUnavailable())
	resp := &catalogpb.GetCollectionByNameResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Collection = model.MarshalCollectionModel(coll)
	}
	return resp, nil
}

func (s *Server) GetCollectionByID(ctx context.Context, req *catalogpb.GetCollectionByIDRequest) (*catalogpb.GetCollectionByIDResponse, error) {
	coll, err := s.meta.GetCollectionByID(ctx, req.GetDbName(), req.GetCollectionId(), req.GetTs(), req.GetAllowUnavailable())
	resp := &catalogpb.GetCollectionByIDResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Collection = model.MarshalCollectionModel(coll)
	}
	return resp, nil
}

func (s *Server) GetCollectionByIDWithMaxTs(ctx context.Context, req *catalogpb.GetCollectionByIDWithMaxTsRequest) (*catalogpb.GetCollectionByIDWithMaxTsResponse, error) {
	coll, err := s.meta.GetCollectionByIDWithMaxTs(ctx, req.GetCollectionId())
	resp := &catalogpb.GetCollectionByIDWithMaxTsResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Collection = model.MarshalCollectionModel(coll)
	}
	return resp, nil
}

func (s *Server) ListCollections(ctx context.Context, req *catalogpb.ListCollectionsRequest) (*catalogpb.ListCollectionsResponse, error) {
	colls, err := s.meta.ListCollections(ctx, req.GetDbName(), req.GetTs(), req.GetOnlyAvail())
	resp := &catalogpb.ListCollectionsResponse{Status: merr.Status(err)}
	if err == nil {
		for _, coll := range colls {
			resp.Collections = append(resp.Collections, model.MarshalCollectionModel(coll))
		}
	}
	return resp, nil
}

func (s *Server) ListAllAvailCollections(ctx context.Context, req *catalogpb.ListAllAvailCollectionsRequest) (*catalogpb.ListAllAvailCollectionsResponse, error) {
	m := s.meta.ListAllAvailCollections(ctx)
	out := make(map[int64]*catalogpb.CollectionInt64List, len(m))
	for dbID, collIDs := range m {
		out[dbID] = &catalogpb.CollectionInt64List{Data: collIDs}
	}
	return &catalogpb.ListAllAvailCollectionsResponse{Status: merr.Success(), Collections: out}, nil
}

func (s *Server) ListAllAvailPartitions(ctx context.Context, req *catalogpb.ListAllAvailPartitionsRequest) (*catalogpb.ListAllAvailPartitionsResponse, error) {
	m := s.meta.ListAllAvailPartitions(ctx)
	out := make(map[int64]*catalogpb.CollectionPartitionMap, len(m))
	for dbID, collMap := range m {
		parts := make(map[int64]*catalogpb.CollectionInt64List, len(collMap))
		for collID, partIDs := range collMap {
			parts[collID] = &catalogpb.CollectionInt64List{Data: partIDs}
		}
		out[dbID] = &catalogpb.CollectionPartitionMap{Partitions: parts}
	}
	return &catalogpb.ListAllAvailPartitionsResponse{Status: merr.Success(), DbPartitions: out}, nil
}

func (s *Server) ListCollectionPhysicalChannels(ctx context.Context, req *catalogpb.ListCollectionPhysicalChannelsRequest) (*catalogpb.ListCollectionPhysicalChannelsResponse, error) {
	m := s.meta.ListCollectionPhysicalChannels(ctx)
	out := make(map[int64]*catalogpb.CollectionStringList, len(m))
	for collID, channels := range m {
		out[collID] = &catalogpb.CollectionStringList{Data: channels}
	}
	return &catalogpb.ListCollectionPhysicalChannelsResponse{Status: merr.Success(), Channels: out}, nil
}

func (s *Server) GetCollectionVirtualChannels(ctx context.Context, req *catalogpb.GetCollectionVirtualChannelsRequest) (*catalogpb.GetCollectionVirtualChannelsResponse, error) {
	channels := s.meta.GetCollectionVirtualChannels(ctx, req.GetCollectionId())
	return &catalogpb.GetCollectionVirtualChannelsResponse{Status: merr.Success(), VirtualChannels: channels}, nil
}

func (s *Server) GetPChannelInfo(ctx context.Context, req *catalogpb.GetPChannelInfoRequest) (*catalogpb.GetPChannelInfoResponse, error) {
	info := s.meta.GetPChannelInfo(ctx, req.GetPchannel())
	return &catalogpb.GetPChannelInfoResponse{Status: merr.Success(), Info: info}, nil
}

func (s *Server) CheckIfCollectionRenamable(ctx context.Context, req *catalogpb.CheckIfCollectionRenamableRequest) (*catalogpb.CheckIfCollectionRenamableResponse, error) {
	err := s.meta.CheckIfCollectionRenamable(ctx, req.GetDbName(), req.GetOldName(), req.GetNewDbName(), req.GetNewName())
	return &catalogpb.CheckIfCollectionRenamableResponse{Status: merr.Status(err)}, nil
}

func (s *Server) BeginTruncateCollection(ctx context.Context, req *catalogpb.BeginTruncateCollectionRequest) (*catalogpb.BeginTruncateCollectionResponse, error) {
	err := s.meta.BeginTruncateCollection(ctx, req.GetCollectionId())
	return &catalogpb.BeginTruncateCollectionResponse{Status: merr.Status(err)}, nil
}

func (s *Server) GetGeneralCount(ctx context.Context, req *catalogpb.GetGeneralCountRequest) (*catalogpb.GetGeneralCountResponse, error) {
	count := s.meta.GetGeneralCount(ctx)
	return &catalogpb.GetGeneralCountResponse{Status: merr.Success(), Count: int64(count)}, nil
}

// AlterCollection [B]: rebuild the BroadcastResult in memory (never broadcast). The meta
// apply reads header, MustBody and result.GetMaxTimeTick(); GetMaxTimeTick scans Results,
// so a single control-channel entry carrying the tick is sufficient.
func (s *Server) AlterCollection(ctx context.Context, req *catalogpb.AlterCollectionRequest) (*catalogpb.AlterCollectionResponse, error) {
	controlChannel := streaming.WAL().ControlChannel()
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(req.GetHeader()).
		WithBody(req.GetBody()).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	result := message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.AlterCollectionMessageHeader, *message.AlterCollectionMessageBody](msg),
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: req.GetTimeTick()},
		},
	}
	err := s.meta.AlterCollection(ctx, result)
	return &catalogpb.AlterCollectionResponse{Status: merr.Status(err)}, nil
}

// TruncateCollection [B]: rebuild the BroadcastResult in memory. The meta apply reads
// header and result.Results[vchannel].TimeTick for every vchannel in the collection's
// ShardInfos, so the per-vchannel time ticks must be carried and rebuilt faithfully.
func (s *Server) TruncateCollection(ctx context.Context, req *catalogpb.TruncateCollectionRequest) (*catalogpb.TruncateCollectionResponse, error) {
	controlChannel := streaming.WAL().ControlChannel()
	vchannels := make([]string, 0, len(req.GetVchannelTimeTicks())+1)
	vchannels = append(vchannels, controlChannel)
	results := map[string]*message.AppendResult{controlChannel: {}}
	for vchannel, tt := range req.GetVchannelTimeTicks() {
		vchannels = append(vchannels, vchannel)
		results[vchannel] = &message.AppendResult{TimeTick: tt}
	}
	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(req.GetHeader()).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()
	result := message.BroadcastResultTruncateCollectionMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.TruncateCollectionMessageHeader, *message.TruncateCollectionMessageBody](msg),
		Results: results,
	}
	err := s.meta.TruncateCollection(ctx, result)
	return &catalogpb.TruncateCollectionResponse{Status: merr.Status(err)}, nil
}
