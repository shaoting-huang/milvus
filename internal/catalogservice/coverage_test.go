package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestAlterAliasBroadcastRoundTrip validates the broadcast reconstruction path: the
// coord-side client extracts (header, control-channel tick) from the BroadcastResult,
// ships them, and the server rebuilds an equivalent BroadcastResult (in memory, never
// transmitting) to drive the real meta.AlterAlias. This is the only non-trivial logic
// in the migration.
func TestAlterAliasBroadcastRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()
	ctrlCh := streaming.WAL().ControlChannel()

	header := &message.AlterAliasMessageHeader{
		DbId:           1,
		DbName:         "default",
		CollectionId:   7777,
		CollectionName: "coll_for_alias",
		Alias:          "myalias",
	}
	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(header).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{ctrlCh}).
		MustBuildBroadcast()
	result := message.BroadcastResultAlterAliasMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.AlterAliasMessageHeader, *message.AlterAliasMessageBody](msg),
		Results: map[string]*message.AppendResult{ctrlCh: {TimeTick: 500}},
	}

	require.NoError(t, meta.AlterAlias(ctx, result))

	// the alias mapping is applied (and persisted): IsAlias routes through the service.
	require.True(t, meta.IsAlias(ctx, "default", "myalias"))
	require.Contains(t, meta.ListAliasesByID(ctx, 7777), "myalias")
}

// TestCollectionRoundTrip validates a non-broadcast model round-trip (Collection <->
// etcd.CollectionInfo) through the service.
func TestCollectionRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()

	coll := &model.Collection{
		CollectionID: 8000,
		Name:         "coll_rt",
		DBName:       "default",
		DBID:         1,
		ShardsNum:    1,
		State:        etcdpb.CollectionState_CollectionCreated,
		CreateTime:   100,
	}
	require.NoError(t, meta.AddCollection(ctx, coll))

	require.Equal(t, int64(8000), meta.GetCollectionID(ctx, "default", "coll_rt"))

	got, err := meta.GetCollectionByName(ctx, "default", "coll_rt", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, "coll_rt", got.Name)
	require.Equal(t, int64(8000), got.CollectionID)
}

// TestRBACRoundTrip validates an RBAC method group (milvuspb entities) through the service.
func TestRBACRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()

	require.NoError(t, meta.CreateRole(ctx, "", &milvuspb.RoleEntity{Name: "role_rt"}))

	roles, err := meta.SelectRole(ctx, "", &milvuspb.RoleEntity{Name: "role_rt"}, false)
	require.NoError(t, err)
	found := false
	for _, r := range roles {
		if r.GetRole().GetName() == "role_rt" {
			found = true
		}
	}
	require.True(t, found, "created role should be selectable through the service")
}
