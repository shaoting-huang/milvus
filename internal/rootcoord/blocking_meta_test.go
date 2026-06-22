package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// readDelegateMeta is a fake IMetaTable that records read delegation; all other methods
// come from the embedded nil IMetaTable (unused by these tests).
type readDelegateMeta struct {
	IMetaTable
	gotRead string
}

func (m *readDelegateMeta) GetDatabaseByName(ctx context.Context, dbName string, ts typeutil.Timestamp) (*model.Database, error) {
	m.gotRead = dbName
	return model.NewDatabase(1, dbName, etcdpb.DatabaseState_DatabaseCreated, nil), nil
}

// blockingMetaTable rejects writes with a retriable error during a migration window while
// letting reads fall through to the source meta (still authoritative until cutover).
func TestBlockingMetaTableRejectsWrites(t *testing.T) {
	src := &readDelegateMeta{}
	blocking := newBlockingMetaTable(src)

	err := blocking.CreateDatabase(context.Background(), model.NewDatabase(2, "x", etcdpb.DatabaseState_DatabaseCreated, nil), 1)
	require.Error(t, err, "writes must be rejected during migration")
	require.True(t, merr.IsRetryableErr(err), "write rejection must be retriable so the proxy retries after cutover")
}

func TestBlockingMetaTablePassesReads(t *testing.T) {
	src := &readDelegateMeta{}
	blocking := newBlockingMetaTable(src)

	db, err := blocking.GetDatabaseByName(context.Background(), "readable", 0)
	require.NoError(t, err, "reads must pass through to the source meta during migration")
	require.Equal(t, "readable", db.Name)
	require.Equal(t, "readable", src.gotRead, "read must have been delegated to the source")
}
