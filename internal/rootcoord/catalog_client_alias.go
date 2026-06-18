package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Alias group ----

// AlterAlias extracts the message header + control-channel time tick from the broadcast
// result and ships them; the service rebuilds the BroadcastResult in memory.
func (r *remoteMetaTable) AlterAlias(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error {
	resp, err := r.client.AlterAlias(ctx, &catalogpb.AlterAliasRequest{
		Header:   result.Message.Header(),
		TimeTick: result.GetControlChannelResult().TimeTick,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

// DropAlias extracts the message header + control-channel time tick from the broadcast
// result and ships them; the service rebuilds the BroadcastResult in memory.
func (r *remoteMetaTable) DropAlias(ctx context.Context, result message.BroadcastResultDropAliasMessageV2) error {
	resp, err := r.client.DropAlias(ctx, &catalogpb.DropAliasRequest{
		Header:   result.Message.Header(),
		TimeTick: result.GetControlChannelResult().TimeTick,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DescribeAlias(ctx context.Context, dbName string, alias string, ts Timestamp) (string, error) {
	resp, err := r.client.DescribeAlias(ctx, &catalogpb.DescribeAliasRequest{DbName: dbName, Alias: alias, Ts: ts})
	if err != nil {
		return "", err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return "", err
	}
	return resp.GetCollectionName(), nil
}

func (r *remoteMetaTable) ListAliases(ctx context.Context, dbName string, collectionName string, ts Timestamp) ([]string, error) {
	resp, err := r.client.ListAliases(ctx, &catalogpb.ListAliasesRequest{DbName: dbName, CollectionName: collectionName, Ts: ts})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp.GetAliases(), nil
}

func (r *remoteMetaTable) IsAlias(ctx context.Context, db, name string) bool {
	resp, err := r.client.IsAlias(ctx, &catalogpb.IsAliasRequest{DbName: db, Name: name})
	if err != nil {
		return false
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return false
	}
	return resp.GetIsAlias()
}

func (r *remoteMetaTable) ListAliasesByID(ctx context.Context, collID UniqueID) []string {
	resp, err := r.client.ListAliasesByID(ctx, &catalogpb.ListAliasesByIDRequest{CollId: collID})
	if err != nil {
		return nil
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil
	}
	return resp.GetAliases()
}

func (r *remoteMetaTable) CheckIfAliasCreatable(ctx context.Context, dbName string, alias string, collectionName string) error {
	resp, err := r.client.CheckIfAliasCreatable(ctx, &catalogpb.CheckIfAliasCreatableRequest{DbName: dbName, Alias: alias, CollectionName: collectionName})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfAliasAlterable(ctx context.Context, dbName string, alias string, collectionName string) error {
	resp, err := r.client.CheckIfAliasAlterable(ctx, &catalogpb.CheckIfAliasAlterableRequest{DbName: dbName, Alias: alias, CollectionName: collectionName})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfAliasDroppable(ctx context.Context, dbName string, alias string) error {
	resp, err := r.client.CheckIfAliasDroppable(ctx, &catalogpb.CheckIfAliasDroppableRequest{DbName: dbName, Alias: alias})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
