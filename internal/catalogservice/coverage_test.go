package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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

// TestDropAliasBroadcastRoundTrip validates the second broadcast write method (the sibling
// of AlterAlias): create an alias through the service, then drop it through the broadcast
// reconstruction path, and confirm the alias is gone from both the in-memory view and the
// persisted catalog. Uses disjoint names from TestAlterAliasBroadcastRoundTrip since the
// real MetaTable is shared across tests.
func TestDropAliasBroadcastRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()
	ctrlCh := streaming.WAL().ControlChannel()

	// arrange: create the alias via AlterAlias (the create path).
	createHeader := &message.AlterAliasMessageHeader{
		DbId:           1,
		DbName:         "default",
		CollectionId:   9001,
		CollectionName: "coll_for_drop",
		Alias:          "drop_me_alias",
	}
	createMsg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(createHeader).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{ctrlCh}).
		MustBuildBroadcast()
	createResult := message.BroadcastResultAlterAliasMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.AlterAliasMessageHeader, *message.AlterAliasMessageBody](createMsg),
		Results: map[string]*message.AppendResult{ctrlCh: {TimeTick: 600}},
	}
	require.NoError(t, meta.AlterAlias(ctx, createResult))
	require.True(t, meta.IsAlias(ctx, "default", "drop_me_alias"))
	require.Contains(t, meta.ListAliasesByID(ctx, 9001), "drop_me_alias")

	// act: drop the alias through the broadcast reconstruction path.
	dropHeader := &message.DropAliasMessageHeader{
		DbId:   1,
		DbName: "default",
		Alias:  "drop_me_alias",
	}
	dropMsg := message.NewDropAliasMessageBuilderV2().
		WithHeader(dropHeader).
		WithBody(&message.DropAliasMessageBody{}).
		WithBroadcast([]string{ctrlCh}).
		MustBuildBroadcast()
	dropResult := message.BroadcastResultDropAliasMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.DropAliasMessageHeader, *message.DropAliasMessageBody](dropMsg),
		Results: map[string]*message.AppendResult{ctrlCh: {TimeTick: 601}},
	}
	require.NoError(t, meta.DropAlias(ctx, dropResult))

	// assert: the alias is gone from the view and from the persisted catalog.
	require.False(t, meta.IsAlias(ctx, "default", "drop_me_alias"))
	require.NotContains(t, meta.ListAliasesByID(ctx, 9001), "drop_me_alias")
}

func fileResourceIDs(list []*internalpb.FileResourceInfo) []int64 {
	ids := make([]int64, 0, len(list))
	for _, r := range list {
		ids = append(ids, r.GetId())
	}
	return ids
}

// TestFileResourceRoundTrip exercises the entire FileResource group (the 6 methods that
// had zero coverage) end-to-end through the service. The key invariant is that the
// in-memory refCnt now lives in the service-side MetaTable and is consulted across gRPC:
// Inc gates a Remove, Dec unblocks it, Recover re-gates it. This proves the refCnt was not
// silently lost when the group was migrated (it has no ctx/error on three of its methods).
func TestFileResourceRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()

	resA := &internalpb.FileResourceInfo{Name: "fr_rt_a", Path: "/p/a", Id: 9100, StorageName: "s3"}
	resB := &internalpb.FileResourceInfo{Name: "fr_rt_b", Path: "/p/b", Id: 9101, StorageName: "s3"}

	// AddFileResource -> visible through ListFileResource, version bumps off zero.
	require.NoError(t, meta.AddFileResource(ctx, resA))
	require.NoError(t, meta.AddFileResource(ctx, resB))
	list, ver := meta.ListFileResource(ctx)
	require.NotZero(t, ver)
	require.Contains(t, fileResourceIDs(list), int64(9100))
	require.Contains(t, fileResourceIDs(list), int64(9101))

	// Inc -> Remove gated by refCnt>0 (removed=false, error returned).
	require.NoError(t, meta.IncFileResourceRefCnt([]int64{9100}))
	err, removed := meta.RemoveFileResource(ctx, "fr_rt_a")
	require.Error(t, err)
	require.False(t, removed)

	// Dec back to 0 -> Remove now succeeds and the resource disappears from the list.
	meta.DecFileResourceRefCnt([]int64{9100})
	err, removed = meta.RemoveFileResource(ctx, "fr_rt_a")
	require.NoError(t, err)
	require.True(t, removed)
	list, _ = meta.ListFileResource(ctx)
	require.NotContains(t, fileResourceIDs(list), int64(9100))

	// Recover re-incs refCnt for a not-yet-persisted collection's resources -> Remove of
	// resB is gated again. collID 99999 is absent from collID2Meta, so the recover counts.
	meta.RecoverFileResourceRefCnt(map[int64][]int64{99999: {9101}})
	err, removed = meta.RemoveFileResource(ctx, "fr_rt_b")
	require.Error(t, err)
	require.False(t, removed)

	// undo the recover-inc and clean up resB.
	meta.DecFileResourceRefCnt([]int64{9101})
	err, removed = meta.RemoveFileResource(ctx, "fr_rt_b")
	require.NoError(t, err)
	require.True(t, removed)

	// RemoveFileResource of an unknown name is a no-op (nil error, removed=false).
	err, removed = meta.RemoveFileResource(ctx, "fr_rt_absent")
	require.NoError(t, err)
	require.False(t, removed)
}

// TestCredentialBroadcastRoundTrip covers two more broadcast write methods (AlterCredential,
// DeleteCredential): both ship a header+body+control-tick that the server rebuilds into a
// BroadcastResult to drive the real permissionLock-guarded meta. AlterCredential creates the
// user when absent; DeleteCredential removes it (its tick must advance past the alter tick,
// since both apply a timetick-versioned write).
func TestCredentialBroadcastRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()
	ctrlCh := streaming.WAL().ControlChannel()

	alterHeader := &message.AlterUserMessageHeader{UserEntity: &milvuspb.UserEntity{Name: "cred_rt"}}
	alterBody := &message.AlterUserMessageBody{CredentialInfo: &internalpb.CredentialInfo{
		Username: "cred_rt", EncryptedPassword: "pw_enc",
	}}
	alterMsg := message.NewAlterUserMessageBuilderV2().
		WithHeader(alterHeader).WithBody(alterBody).WithBroadcast([]string{ctrlCh}).MustBuildBroadcast()
	alterResult := message.BroadcastResultAlterUserMessageV2{
		Message: message.MustAsBroadcastAlterUserMessageV2(alterMsg),
		Results: map[string]*message.AppendResult{ctrlCh: {TimeTick: 700}},
	}
	require.NoError(t, meta.AlterCredential(ctx, alterResult))

	got, err := meta.GetCredential(ctx, "cred_rt")
	require.NoError(t, err)
	require.Equal(t, "pw_enc", got.EncryptedPassword)

	dropHeader := &message.DropUserMessageHeader{UserName: "cred_rt"}
	dropMsg := message.NewDropUserMessageBuilderV2().
		WithHeader(dropHeader).WithBody(&message.DropUserMessageBody{}).WithBroadcast([]string{ctrlCh}).MustBuildBroadcast()
	dropResult := message.BroadcastResultDropUserMessageV2{
		Message: message.MustAsBroadcastDropUserMessageV2(dropMsg),
		Results: map[string]*message.AppendResult{ctrlCh: {TimeTick: 701}},
	}
	require.NoError(t, meta.DeleteCredential(ctx, dropResult))

	_, err = meta.GetCredential(ctx, "cred_rt")
	require.Error(t, err, "credential must be gone after DeleteCredential")
}

// TestAlterCollectionBroadcastRoundTrip covers the AlterCollection broadcast write: the header
// (collection id + field mask) and body (updates) ride the wire and the server rebuilds the
// BroadcastResult to drive the real meta. Here we flip the description through the field mask.
func TestAlterCollectionBroadcastRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()
	ctrlCh := streaming.WAL().ControlChannel()

	coll := &model.Collection{
		CollectionID: 9200, Name: "alter_rt", DBName: "default", DBID: 1, ShardsNum: 1,
		State: etcdpb.CollectionState_CollectionCreated, CreateTime: 100, Description: "orig",
	}
	require.NoError(t, meta.AddCollection(ctx, coll))

	alterHeader := &message.AlterCollectionMessageHeader{
		CollectionId: 9200,
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionDescription}},
	}
	alterBody := &message.AlterCollectionMessageBody{
		Updates: &messagespb.AlterCollectionMessageUpdates{Description: "updated"},
	}
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(alterHeader).WithBody(alterBody).WithBroadcast([]string{ctrlCh}).MustBuildBroadcast()
	result := message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.AlterCollectionMessageHeader, *message.AlterCollectionMessageBody](msg),
		Results: map[string]*message.AppendResult{ctrlCh: {TimeTick: 710}},
	}
	require.NoError(t, meta.AlterCollection(ctx, result))

	got, err := meta.GetCollectionByName(ctx, "default", "alter_rt", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, "updated", got.Description)
}

// TestTruncateCollectionBroadcastRoundTrip covers the last broadcast write (TruncateCollection),
// whose wire shape is unique: per-vchannel time ticks. The client ships a vchannel->tick map and
// the server rebuilds the results map keyed by vchannel. meta.TruncateCollection then indexes
// result.Results[vchannel] for every shard, so a dropped vchannel would nil-panic — success
// proves the per-vchannel map survived the round trip faithfully.
func TestTruncateCollectionBroadcastRoundTrip(t *testing.T) {
	meta := remoteMeta(t)
	ctx := context.Background()
	ctrlCh := streaming.WAL().ControlChannel()
	const vch = "by-dev-rootcoord-dml_trunc_9300v0"
	const pch = "by-dev-rootcoord-dml_trunc"

	coll := &model.Collection{
		CollectionID: 9300, Name: "trunc_rt", DBName: "default", DBID: 1, ShardsNum: 1,
		State: etcdpb.CollectionState_CollectionCreated, CreateTime: 100,
		VirtualChannelNames:  []string{vch},
		PhysicalChannelNames: []string{pch},
		ShardInfos: map[string]*model.ShardInfo{
			vch: {VChannelName: vch, PChannelName: pch, LastTruncateTimeTick: 0},
		},
	}
	require.NoError(t, meta.AddCollection(ctx, coll))

	// Begin sets the on-truncating marker (a non-broadcast write through the service).
	require.NoError(t, meta.BeginTruncateCollection(ctx, 9300))

	header := &message.TruncateCollectionMessageHeader{DbId: 1, CollectionId: 9300}
	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(header).WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast([]string{ctrlCh, vch}).MustBuildBroadcast()
	result := message.BroadcastResultTruncateCollectionMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.TruncateCollectionMessageHeader, *message.TruncateCollectionMessageBody](msg),
		Results: map[string]*message.AppendResult{ctrlCh: {}, vch: {TimeTick: 720}},
	}
	require.NoError(t, meta.TruncateCollection(ctx, result))

	// the per-vchannel tick landed on the shard, proving the map round-tripped.
	got, err := meta.GetCollectionByName(ctx, "default", "trunc_rt", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, uint64(720), got.ShardInfos[vch].LastTruncateTimeTick)
}
