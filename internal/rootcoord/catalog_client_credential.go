package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Credential group ----

func (r *remoteMetaTable) InitCredential(ctx context.Context) error {
	resp, err := r.client.InitCredential(ctx, &catalogpb.InitCredentialRequest{})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) AlterCredential(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error {
	var timeTick uint64
	if ccResult := result.GetControlChannelResult(); ccResult != nil {
		timeTick = ccResult.TimeTick
	}
	resp, err := r.client.AlterCredential(ctx, &catalogpb.AlterCredentialRequest{
		Header:   result.Message.Header(),
		Body:     result.Message.MustBody(),
		TimeTick: timeTick,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DeleteCredential(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error {
	var timeTick uint64
	if ccResult := result.GetControlChannelResult(); ccResult != nil {
		timeTick = ccResult.TimeTick
	}
	resp, err := r.client.DeleteCredential(ctx, &catalogpb.DeleteCredentialRequest{
		Header:   result.Message.Header(),
		TimeTick: timeTick,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) GetCredential(ctx context.Context, username string) (*internalpb.CredentialInfo, error) {
	resp, err := r.client.GetCredential(ctx, &catalogpb.GetCredentialRequest{Username: username})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetCredential(), nil
}

func (r *remoteMetaTable) ListCredentialUsernames(ctx context.Context) (*milvuspb.ListCredUsersResponse, error) {
	resp, err := r.client.ListCredentialUsernames(ctx, &catalogpb.ListCredentialUsernamesRequest{})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return &milvuspb.ListCredUsersResponse{Usernames: resp.GetUsernames()}, nil
}

func (r *remoteMetaTable) CheckIfAddCredential(ctx context.Context, req *internalpb.CredentialInfo) error {
	resp, err := r.client.CheckIfAddCredential(ctx, &catalogpb.CheckIfAddCredentialRequest{CredInfo: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfUpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) error {
	resp, err := r.client.CheckIfUpdateCredential(ctx, &catalogpb.CheckIfUpdateCredentialRequest{CredInfo: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfDeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) error {
	resp, err := r.client.CheckIfDeleteCredential(ctx, &catalogpb.CheckIfDeleteCredentialRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
