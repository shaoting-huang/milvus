package rootcoord

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func migEtcdClient(t *testing.T) *clientv3.Client {
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(ep, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

var migTSO int64

func migTSOAlloc() tso.Allocator {
	return &tso.MockAllocator{GenerateTSOF: func(count uint32) (uint64, error) {
		migTSO += int64(count)
		return uint64(migTSO), nil
	}}
}

// fakeBulkClient stands in for the catalog service: BulkImport records the snapshot,
// VerifyImport returns whatever mismatches the test wants. Lets the orchestrator's gate /
// drain / rollback logic be tested without a real gRPC server (which would create an import
// cycle); the real wire path is covered by the catalogservice package e2e.
type fakeBulkClient struct {
	imported     map[string]string
	verifyResult []*catalogpb.VerifyMismatch
	importErr    error
}

func (f *fakeBulkClient) BulkImport(ctx context.Context, in *catalogpb.BulkImportRequest, _ ...grpc.CallOption) (*catalogpb.BulkImportResponse, error) {
	if f.importErr != nil {
		return nil, f.importErr
	}
	f.imported = make(map[string]string, len(in.GetEntries()))
	for _, e := range in.GetEntries() {
		f.imported[e.GetKey()] = string(e.GetValue())
	}
	return &catalogpb.BulkImportResponse{Status: merr.Success(), Imported: int64(len(f.imported))}, nil
}

func (f *fakeBulkClient) VerifyImport(ctx context.Context, in *catalogpb.VerifyImportRequest, _ ...grpc.CallOption) (*catalogpb.VerifyImportResponse, error) {
	return &catalogpb.VerifyImportResponse{Status: merr.Success(), Mismatches: f.verifyResult}, nil
}

func newSourceMeta(t *testing.T, root string) (*MetaTable, kv.MetaKv) {
	srcKV := etcdkv.NewEtcdKV(migEtcdClient(t), root)
	require.NoError(t, srcKV.RemoveWithPrefix(context.Background(), ""))
	m, err := NewMetaTable(context.Background(), kvrootcoord.NewCatalog(srcKV), migTSOAlloc())
	require.NoError(t, err)
	return m, srcKV
}

// TestRuntimeMigrationHappyPath: gate -> drain -> bulk-import via client -> verify clean ->
// cut over. The pre-migration db is visible through the switched meta afterwards. The coord
// only read its own source; the destination was written through the client.
func TestRuntimeMigrationHappyPath(t *testing.T) {
	source, srcKV := newSourceMeta(t, "by-dev/mig-happy-src")
	require.NoError(t, source.CreateDatabase(context.Background(), model.NewDatabase(7000, "premig_db", etcdpb.DatabaseState_DatabaseCreated, nil), 100))

	sw := NewSwitchableMetaTable(source)
	fake := &fakeBulkClient{}
	cfg := MigrationConfig{
		Switchable: sw, Source: source, SourceKV: srcKV,
		Roots: []string{"root-coord"}, Namespace: "clusterA", Client: fake,
		BuildRemote: func() (IMetaTable, error) { return source, nil }, // stand-in cutover target
	}
	require.NoError(t, Migrate(context.Background(), cfg))

	// the client received the source snapshot (db landed in the imported set).
	require.NotEmpty(t, fake.imported)
	found := false
	for k := range fake.imported {
		if strings.Contains(k, "database/db-info/7000") {
			found = true
		}
	}
	require.True(t, found, "source db must have been shipped to the service via BulkImport")
}

// TestRuntimeMigrationRollbackOnVerify: the service reports a mismatch -> migration rolls
// back, c.meta stays on the source, writes resume.
func TestRuntimeMigrationRollbackOnVerify(t *testing.T) {
	source, srcKV := newSourceMeta(t, "by-dev/mig-rb-src")
	require.NoError(t, source.CreateDatabase(context.Background(), model.NewDatabase(7001, "rb_db", etcdpb.DatabaseState_DatabaseCreated, nil), 100))

	sw := NewSwitchableMetaTable(source)
	fake := &fakeBulkClient{verifyResult: []*catalogpb.VerifyMismatch{{Key: "root-coord/x", Reason: "value-differs"}}}
	cfg := MigrationConfig{
		Switchable: sw, Source: source, SourceKV: srcKV,
		Roots: []string{"root-coord"}, Namespace: "clusterB", Client: fake,
		BuildRemote: func() (IMetaTable, error) {
			t.Fatal("BuildRemote must not be called when verify reports mismatches")
			return nil, nil
		},
	}
	require.Error(t, Migrate(context.Background(), cfg), "verify mismatch must fail migration")

	require.Same(t, IMetaTable(source), sw.Current(), "must remain on the source after rollback")
	require.NoError(t, sw.CreateDatabase(context.Background(), model.NewDatabase(7002, "post_rb", etcdpb.DatabaseState_DatabaseCreated, nil), 101))
}

// TestRuntimeMigrationGatesWrites: mid-migration (after import, before cutover) writes are
// rejected with a retriable error.
func TestRuntimeMigrationGatesWrites(t *testing.T) {
	source, srcKV := newSourceMeta(t, "by-dev/mig-gate-src")
	sw := NewSwitchableMetaTable(source)

	var writeErr error
	fake := &fakeBulkClient{}
	cfg := MigrationConfig{
		Switchable: sw, Source: source, SourceKV: srcKV,
		Roots: []string{"root-coord"}, Namespace: "clusterC", Client: fake,
		BuildRemote: func() (IMetaTable, error) { return source, nil },
		afterCopyHook: func() {
			writeErr = sw.CreateDatabase(context.Background(), model.NewDatabase(7003, "blocked", etcdpb.DatabaseState_DatabaseCreated, nil), 102)
		},
	}
	require.NoError(t, Migrate(context.Background(), cfg))
	require.Error(t, writeErr, "writes during migration must be rejected")
	require.True(t, merr.IsRetryableErr(writeErr), "rejection must be retriable")
}
