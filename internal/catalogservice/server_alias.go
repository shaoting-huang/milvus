package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Alias group ----

// AlterAlias rebuilds the BroadcastResultAlterAliasMessageV2 in memory (no broadcast)
// from the request header + control-channel time tick, then calls the real meta.
func (s *Server) AlterAlias(ctx context.Context, req *catalogpb.AlterAliasRequest) (*catalogpb.AlterAliasResponse, error) {
	controlChannel := streaming.WAL().ControlChannel()
	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(req.GetHeader()).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	result := message.BroadcastResultAlterAliasMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.AlterAliasMessageHeader, *message.AlterAliasMessageBody](msg),
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: req.GetTimeTick()},
		},
	}
	err := s.meta.AlterAlias(ctx, result)
	return &catalogpb.AlterAliasResponse{Status: merr.Status(err)}, nil
}

// DropAlias rebuilds the BroadcastResultDropAliasMessageV2 in memory (no broadcast)
// from the request header + control-channel time tick, then calls the real meta.
func (s *Server) DropAlias(ctx context.Context, req *catalogpb.DropAliasRequest) (*catalogpb.DropAliasResponse, error) {
	controlChannel := streaming.WAL().ControlChannel()
	msg := message.NewDropAliasMessageBuilderV2().
		WithHeader(req.GetHeader()).
		WithBody(&message.DropAliasMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	result := message.BroadcastResultDropAliasMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.DropAliasMessageHeader, *message.DropAliasMessageBody](msg),
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: req.GetTimeTick()},
		},
	}
	err := s.meta.DropAlias(ctx, result)
	return &catalogpb.DropAliasResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DescribeAlias(ctx context.Context, req *catalogpb.DescribeAliasRequest) (*catalogpb.DescribeAliasResponse, error) {
	collectionName, err := s.meta.DescribeAlias(ctx, req.GetDbName(), req.GetAlias(), req.GetTs())
	resp := &catalogpb.DescribeAliasResponse{Status: merr.Status(err)}
	if err == nil {
		resp.CollectionName = collectionName
	}
	return resp, nil
}

func (s *Server) ListAliases(ctx context.Context, req *catalogpb.ListAliasesRequest) (*catalogpb.ListAliasesResponse, error) {
	aliases, err := s.meta.ListAliases(ctx, req.GetDbName(), req.GetCollectionName(), req.GetTs())
	resp := &catalogpb.ListAliasesResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Aliases = aliases
	}
	return resp, nil
}

func (s *Server) IsAlias(ctx context.Context, req *catalogpb.IsAliasRequest) (*catalogpb.IsAliasResponse, error) {
	isAlias := s.meta.IsAlias(ctx, req.GetDbName(), req.GetName())
	return &catalogpb.IsAliasResponse{Status: merr.Status(nil), IsAlias: isAlias}, nil
}

func (s *Server) ListAliasesByID(ctx context.Context, req *catalogpb.ListAliasesByIDRequest) (*catalogpb.ListAliasesByIDResponse, error) {
	aliases := s.meta.ListAliasesByID(ctx, req.GetCollId())
	return &catalogpb.ListAliasesByIDResponse{Status: merr.Status(nil), Aliases: aliases}, nil
}

func (s *Server) CheckIfAliasCreatable(ctx context.Context, req *catalogpb.CheckIfAliasCreatableRequest) (*catalogpb.CheckIfAliasCreatableResponse, error) {
	err := s.meta.CheckIfAliasCreatable(ctx, req.GetDbName(), req.GetAlias(), req.GetCollectionName())
	return &catalogpb.CheckIfAliasCreatableResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfAliasAlterable(ctx context.Context, req *catalogpb.CheckIfAliasAlterableRequest) (*catalogpb.CheckIfAliasAlterableResponse, error) {
	err := s.meta.CheckIfAliasAlterable(ctx, req.GetDbName(), req.GetAlias(), req.GetCollectionName())
	return &catalogpb.CheckIfAliasAlterableResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfAliasDroppable(ctx context.Context, req *catalogpb.CheckIfAliasDroppableRequest) (*catalogpb.CheckIfAliasDroppableResponse, error) {
	err := s.meta.CheckIfAliasDroppable(ctx, req.GetDbName(), req.GetAlias())
	return &catalogpb.CheckIfAliasDroppableResponse{Status: merr.Status(err)}, nil
}
