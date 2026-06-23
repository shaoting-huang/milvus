package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// importServer builds a Server whose import KV resolver gives each namespace its own TiKV
// prefix — the same isolation the runtime path uses. No MetaTable needed for these tests.
func importServer() *Server {
	return NewServer(nil, WithImportKV(func(namespace string) kv.MetaKv {
		return tikvkv.NewTiKV(tikvClient, "by-dev/srv-mig/"+namespace)
	}))
}

func entries(kvs map[string]string) []*catalogpb.KvEntry {
	out := make([]*catalogpb.KvEntry, 0, len(kvs))
	for k, v := range kvs {
		out = append(out, &catalogpb.KvEntry{Key: k, Value: []byte(v)})
	}
	return out
}

// TestBulkImportThenVerifyClean: imported entries land in the namespace's backend and a
// verify against the same snapshot reports no mismatch.
func TestBulkImportThenVerifyClean(t *testing.T) {
	srv := importServer()
	ctx := context.Background()
	ns := "bulkA"
	// clean the namespace prefix first
	require.NoError(t, tikvkv.NewTiKV(tikvClient, "by-dev/srv-mig/"+ns).RemoveWithPrefix(ctx, ""))

	snap := map[string]string{
		"root-coord/database/db-info/1": "db1",
		"root-coord/collection/100":     "coll100",
	}
	imp, err := srv.BulkImport(ctx, &catalogpb.BulkImportRequest{Namespace: ns, Entries: entries(snap)})
	require.NoError(t, err)
	require.NoError(t, merr.Error(imp.GetStatus()))
	require.Equal(t, int64(2), imp.GetImported())

	ver, err := srv.VerifyImport(ctx, &catalogpb.VerifyImportRequest{Namespace: ns, Roots: []string{"root-coord"}, Entries: entries(snap)})
	require.NoError(t, err)
	require.NoError(t, merr.Error(ver.GetStatus()))
	require.Empty(t, ver.GetMismatches(), "imported snapshot must verify clean")
}

// TestVerifyImportDetectsMismatch: a source snapshot that diverges from what was imported
// is reported (the safety net that aborts a migration before cutover).
func TestVerifyImportDetectsMismatch(t *testing.T) {
	srv := importServer()
	ctx := context.Background()
	ns := "bulkB"
	require.NoError(t, tikvkv.NewTiKV(tikvClient, "by-dev/srv-mig/"+ns).RemoveWithPrefix(ctx, ""))

	imported := map[string]string{"root-coord/collection/1": "v1"}
	_, err := srv.BulkImport(ctx, &catalogpb.BulkImportRequest{Namespace: ns, Entries: entries(imported)})
	require.NoError(t, err)

	// a fresh source snapshot with a changed value + an extra key not in dest.
	freshSrc := map[string]string{
		"root-coord/collection/1": "v1-CHANGED",
		"root-coord/collection/2": "v2-new",
	}
	ver, err := srv.VerifyImport(ctx, &catalogpb.VerifyImportRequest{Namespace: ns, Roots: []string{"root-coord"}, Entries: entries(freshSrc)})
	require.NoError(t, err)
	require.NotEmpty(t, ver.GetMismatches(), "divergent snapshot must be reported")
}

// TestMigrationDisabledWithoutImportKV: a Server with no import-KV resolver must reject the
// migration RPCs through a clean error status (never panic, never silently no-op) so the coord
// aborts the migration and rolls back instead of cutting over to an empty backend.
func TestMigrationDisabledWithoutImportKV(t *testing.T) {
	srv := NewServer(nil) // no WithImportKV: migration capability disabled
	ctx := context.Background()

	imp, err := srv.BulkImport(ctx, &catalogpb.BulkImportRequest{Namespace: "x", Entries: entries(map[string]string{"k": "v"})})
	require.NoError(t, err) // transport ok; the failure rides in the status
	require.Error(t, merr.Error(imp.GetStatus()), "bulk import without a backend must report an error status")

	ver, err := srv.VerifyImport(ctx, &catalogpb.VerifyImportRequest{Namespace: "x", Roots: []string{"root-coord"}})
	require.NoError(t, err)
	require.Error(t, merr.Error(ver.GetStatus()), "verify without a backend must report an error status")
}
