package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Database precondition checks (dbchecks group) ----

func (r *remoteMetaTable) CheckIfDatabaseCreatable(ctx context.Context, req *milvuspb.CreateDatabaseRequest) error {
	resp, err := r.client.CheckIfDatabaseCreatable(ctx, &catalogpb.CheckIfDatabaseCreatableRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) CheckIfDatabaseDroppable(ctx context.Context, req *milvuspb.DropDatabaseRequest) error {
	resp, err := r.client.CheckIfDatabaseDroppable(ctx, &catalogpb.CheckIfDatabaseDroppableRequest{Req: req})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}
