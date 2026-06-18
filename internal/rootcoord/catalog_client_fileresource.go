package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- FileResource ----

func (r *remoteMetaTable) AddFileResource(ctx context.Context, resource *internalpb.FileResourceInfo) error {
	resp, err := r.client.AddFileResource(ctx, &catalogpb.AddFileResourceRequest{Resource: resource})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) RemoveFileResource(ctx context.Context, name string) (error, bool) {
	resp, err := r.client.RemoveFileResource(ctx, &catalogpb.RemoveFileResourceRequest{Name: name})
	if err != nil {
		return err, false
	}
	return merr.Error(resp.GetStatus()), resp.GetRemoved()
}

func (r *remoteMetaTable) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64) {
	resp, err := r.client.ListFileResource(ctx, &catalogpb.ListFileResourceRequest{})
	if err != nil {
		return nil, 0
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, 0
	}
	return resp.GetResources(), resp.GetVersion()
}

func (r *remoteMetaTable) IncFileResourceRefCnt(ids []int64) error {
	resp, err := r.client.IncFileResourceRefCnt(context.Background(), &catalogpb.IncFileResourceRefCntRequest{Ids: ids})
	if err != nil {
		return err
	}
	return merr.Error(resp.GetStatus())
}

func (r *remoteMetaTable) DecFileResourceRefCnt(ids []int64) {
	_, _ = r.client.DecFileResourceRefCnt(context.Background(), &catalogpb.DecFileResourceRefCntRequest{Ids: ids})
}

func (r *remoteMetaTable) RecoverFileResourceRefCnt(pendingCollections map[int64][]int64) {
	m := make(map[int64]*catalogpb.FileResourceInt64List, len(pendingCollections))
	for k, v := range pendingCollections {
		m[k] = &catalogpb.FileResourceInt64List{Ids: v}
	}
	_, _ = r.client.RecoverFileResourceRefCnt(context.Background(), &catalogpb.RecoverFileResourceRefCntRequest{PendingCollections: m})
}
