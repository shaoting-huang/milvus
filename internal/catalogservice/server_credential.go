package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Credential group ----
//
// The two broadcast-driven mutations rebuild the streaming BroadcastResult IN MEMORY
// (they never broadcast); the control-channel key comes from streaming.WAL().ControlChannel()
// so the meta method's GetControlChannelResult()/GetMaxTimeTick() observe the carried tick.

func (s *Server) InitCredential(ctx context.Context, req *catalogpb.InitCredentialRequest) (*catalogpb.InitCredentialResponse, error) {
	err := s.meta.InitCredential(ctx)
	return &catalogpb.InitCredentialResponse{Status: merr.Status(err)}, nil
}

func (s *Server) AlterCredential(ctx context.Context, req *catalogpb.AlterCredentialRequest) (*catalogpb.AlterCredentialResponse, error) {
	controlChannel := streaming.WAL().ControlChannel()
	msg := message.NewAlterUserMessageBuilderV2().
		WithHeader(req.GetHeader()).
		WithBody(req.GetBody()).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	result := message.BroadcastResultAlterUserMessageV2{
		Message: message.MustAsBroadcastAlterUserMessageV2(msg),
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: req.GetTimeTick()},
		},
	}
	err := s.meta.AlterCredential(ctx, result)
	return &catalogpb.AlterCredentialResponse{Status: merr.Status(err)}, nil
}

func (s *Server) DeleteCredential(ctx context.Context, req *catalogpb.DeleteCredentialRequest) (*catalogpb.DeleteCredentialResponse, error) {
	controlChannel := streaming.WAL().ControlChannel()
	msg := message.NewDropUserMessageBuilderV2().
		WithHeader(req.GetHeader()).
		WithBody(&message.DropUserMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	result := message.BroadcastResultDropUserMessageV2{
		Message: message.MustAsBroadcastDropUserMessageV2(msg),
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: req.GetTimeTick()},
		},
	}
	err := s.meta.DeleteCredential(ctx, result)
	return &catalogpb.DeleteCredentialResponse{Status: merr.Status(err)}, nil
}

func (s *Server) GetCredential(ctx context.Context, req *catalogpb.GetCredentialRequest) (*catalogpb.GetCredentialResponse, error) {
	cred, err := s.meta.GetCredential(ctx, req.GetUsername())
	resp := &catalogpb.GetCredentialResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Credential = cred
	}
	return resp, nil
}

func (s *Server) ListCredentialUsernames(ctx context.Context, req *catalogpb.ListCredentialUsernamesRequest) (*catalogpb.ListCredentialUsernamesResponse, error) {
	usernames, err := s.meta.ListCredentialUsernames(ctx)
	resp := &catalogpb.ListCredentialUsernamesResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Usernames = usernames.GetUsernames()
	}
	return resp, nil
}

func (s *Server) CheckIfAddCredential(ctx context.Context, req *catalogpb.CheckIfAddCredentialRequest) (*catalogpb.CheckIfAddCredentialResponse, error) {
	err := s.meta.CheckIfAddCredential(ctx, req.GetCredInfo())
	return &catalogpb.CheckIfAddCredentialResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfUpdateCredential(ctx context.Context, req *catalogpb.CheckIfUpdateCredentialRequest) (*catalogpb.CheckIfUpdateCredentialResponse, error) {
	err := s.meta.CheckIfUpdateCredential(ctx, req.GetCredInfo())
	return &catalogpb.CheckIfUpdateCredentialResponse{Status: merr.Status(err)}, nil
}

func (s *Server) CheckIfDeleteCredential(ctx context.Context, req *catalogpb.CheckIfDeleteCredentialRequest) (*catalogpb.CheckIfDeleteCredentialResponse, error) {
	err := s.meta.CheckIfDeleteCredential(ctx, req.GetReq())
	return &catalogpb.CheckIfDeleteCredentialResponse{Status: merr.Status(err)}, nil
}
