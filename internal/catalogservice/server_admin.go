package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---- Admin / discovery ----

// GetRouteMap returns the membership + shard->owner map so a client can route each namespace
// to its owner. Clients call this on any node, keeping the pooled etcd unexposed.
func (s *Server) GetRouteMap(ctx context.Context, req *catalogpb.GetRouteMapRequest) (*catalogpb.GetRouteMapResponse, error) {
	if s.routeProvider == nil {
		return &catalogpb.GetRouteMapResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg("routing not enabled"))}, nil
	}
	members, shardOwner, err := s.routeProvider.RouteMap(ctx)
	resp := &catalogpb.GetRouteMapResponse{Status: merr.Status(err)}
	if err == nil {
		resp.Members = members
		resp.ShardOwner = make(map[int32]string, len(shardOwner))
		for shard, owner := range shardOwner {
			resp.ShardOwner[int32(shard)] = owner
		}
	}
	return resp, nil
}

// DeleteNamespace evicts a namespace's cached MetaTable on this node (e.g. when the cluster
// is removed). Routing is unaffected — the shard slots are fixed.
func (s *Server) DeleteNamespace(ctx context.Context, req *catalogpb.DeleteNamespaceRequest) (*catalogpb.DeleteNamespaceResponse, error) {
	if s.registry != nil {
		s.registry.Evict(req.GetNamespace())
	}
	return &catalogpb.DeleteNamespaceResponse{Status: merr.Success()}, nil
}
