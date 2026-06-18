package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Partition DDL ----

func (r *remoteMetaTable) AddPartition(ctx context.Context, partition *model.Partition) error {
	resp, err := r.client.AddPartition(ctx, &catalogpb.AddPartitionRequest{Partition: model.MarshalPartitionModel(partition)})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) GetPartitionIDByName(collectionID int64, partitionName string) (int64, bool) {
	resp, err := r.client.GetPartitionIDByName(context.Background(), &catalogpb.GetPartitionIDByNameRequest{
		CollectionId:  collectionID,
		PartitionName: partitionName,
	})
	if err != nil {
		return 0, false
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return 0, false
	}
	return resp.GetPartitionId(), resp.GetExists()
}

func (r *remoteMetaTable) DropPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	resp, err := r.client.DropPartition(ctx, &catalogpb.DropPartitionRequest{
		CollectionId: collectionID,
		PartitionId:  partitionID,
		Ts:           ts,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	resp, err := r.client.RemovePartition(ctx, &catalogpb.RemovePartitionRequest{
		CollectionId: collectionID,
		PartitionId:  partitionID,
		Ts:           ts,
	})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
