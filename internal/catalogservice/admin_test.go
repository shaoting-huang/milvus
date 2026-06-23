package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeRouteProvider struct {
	members    []string
	shardOwner map[int]string
	shardTerm  map[int]int64
}

func (f fakeRouteProvider) RouteMap(context.Context) ([]string, map[int]string, map[int]int64, error) {
	return f.members, f.shardOwner, f.shardTerm, nil
}

// TestGetRouteMap: the service returns members + shard->owner so a client can discover owners
// without touching etcd.
func TestGetRouteMap(t *testing.T) {
	srv := NewServer(nil, WithRouteProvider(fakeRouteProvider{
		members:    []string{"node-a", "node-b"},
		shardOwner: map[int]string{0: "node-a", 1: "node-b"},
		shardTerm:  map[int]int64{0: 11, 1: 22},
	}))
	resp, err := srv.GetRouteMap(context.Background(), &catalogpb.GetRouteMapRequest{})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.ElementsMatch(t, []string{"node-a", "node-b"}, resp.GetMembers())
	require.Equal(t, "node-a", resp.GetShardOwner()[0])
	require.Equal(t, "node-b", resp.GetShardOwner()[1])
	require.Equal(t, int64(11), resp.GetShardTerm()[0])
	require.Equal(t, int64(22), resp.GetShardTerm()[1])
}

// TestDeleteNamespaceEvicts: deleting a namespace drops its cached MetaTable so a later Get
// rebuilds from the backend.
func TestDeleteNamespaceEvicts(t *testing.T) {
	reg := newTestRegistry()
	srv := NewServer(nil, WithRegistry(reg))

	m1, err := reg.Get("ns_del", 0)
	require.NoError(t, err)

	_, err = srv.DeleteNamespace(context.Background(), &catalogpb.DeleteNamespaceRequest{Namespace: "ns_del"})
	require.NoError(t, err)

	m2, err := reg.Get("ns_del", 0)
	require.NoError(t, err)
	require.NotSame(t, m1, m2, "namespace must be rebuilt after deletion/eviction")
}
