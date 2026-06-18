package catalogservice

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tikv/client-go/v2/testutils"
	tilib "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	etcdpb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// tikvClient is the catalog backend for the PoC. By default it is an in-process mock
// TiKV (REAL client-go v2 txnkv semantics, in-memory store) — the design backend is
// TiKV, NOT etcd. Set TIKV_PD=<pd-addr> to point at a real TiKV cluster.
var tikvClient *txnkv.Client

func TestMain(m *testing.M) {
	paramtable.Init()
	streaming.SetupNoopWALForTest() // broadcast server methods call streaming.WAL().ControlChannel()
	tikvClient = setupTiKV()
	os.Exit(m.Run())
}

func setupTiKV() *txnkv.Client {
	if pd := os.Getenv("TIKV_PD"); pd != "" {
		cli, err := txnkv.NewClient([]string{pd})
		if err != nil {
			panic(err)
		}
		return cli
	}
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	if err != nil {
		panic(err)
	}
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tilib.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	if err != nil {
		panic(err)
	}
	return &txnkv.Client{KVStore: store}
}

var tsoCounter atomic.Uint64

func newMockTSO() tso.Allocator {
	return &tso.MockAllocator{
		GenerateTSOF: func(count uint32) (uint64, error) {
			return tsoCounter.Add(uint64(count)), nil
		},
	}
}

// The MetaTable is built exactly ONCE per process: reload() touches the global streaming
// pchannel-stats singleton, which is not re-entrant. The single real MetaTable + service
// is shared across tests; tests use disjoint db names. Persisted truth is read directly
// from the catalog, never from a second MetaTable.
const pocRootPath = "by-dev/catalog-poc"

var (
	pocOnce     sync.Once
	pocClient   catalogpb.CatalogServiceClient
	pocSetupErr error
)

func getClient(t *testing.T) catalogpb.CatalogServiceClient {
	pocOnce.Do(func() {
		mkv := tikvkv.NewTiKV(tikvClient, pocRootPath)
		if err := mkv.RemoveWithPrefix(context.Background(), ""); err != nil {
			pocSetupErr = err
			return
		}
		meta, err := rootcoord.NewMetaTable(context.Background(), kvrootcoord.NewCatalog(mkv), newMockTSO())
		if err != nil {
			pocSetupErr = err
			return
		}
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			pocSetupErr = err
			return
		}
		srv := grpc.NewServer()
		catalogpb.RegisterCatalogServiceServer(srv, NewServer(meta))
		go func() { _ = srv.Serve(lis) }()
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			pocSetupErr = err
			return
		}
		pocClient = catalogpb.NewCatalogServiceClient(conn)
	})
	require.NoError(t, pocSetupErr)
	return pocClient
}

// remoteMeta returns the coord-side thin client (the exact type of Core.meta) backed by
// the running service, with a nil fallback (tests only exercise migrated methods).
func remoteMeta(t *testing.T) rootcoord.IMetaTable {
	return rootcoord.NewRemoteMetaTable(nil, getClient(t))
}

func dbModel(id int64, name string) *model.Database {
	return model.NewDatabase(id, name, etcdpb.DatabaseState_DatabaseCreated, nil)
}

// persistedDBNames reads the PERSISTED TRUTH straight from the catalog (TiKV).
func persistedDBNames(t *testing.T) []string {
	catalog := kvrootcoord.NewCatalog(tikvkv.NewTiKV(tikvClient, pocRootPath))
	dbs, err := catalog.ListDatabases(context.Background(), typeutil.MaxTimestamp)
	require.NoError(t, err)
	names := make([]string, 0, len(dbs))
	for _, db := range dbs {
		names = append(names, db.Name)
	}
	sort.Strings(names)
	return names
}

// TestDatabaseEquivalence: a CreateDatabase through the coord-side IMetaTable routes over
// gRPC to the service, is visible through service reads, and is persisted in TiKV.
func TestDatabaseEquivalence(t *testing.T) {
	meta := remoteMeta(t)

	require.NoError(t, meta.CreateDatabase(context.Background(), dbModel(1000, "db_eq"), 100))

	got, err := meta.GetDatabaseByName(context.Background(), "db_eq", 0)
	require.NoError(t, err)
	require.Equal(t, "db_eq", got.Name)
	require.Equal(t, int64(1000), got.ID)

	dbs, err := meta.ListDatabases(context.Background(), 0)
	require.NoError(t, err)
	names := make([]string, 0, len(dbs))
	for _, d := range dbs {
		names = append(names, d.Name)
	}
	require.Contains(t, names, "db_eq")

	require.Contains(t, persistedDBNames(t), "db_eq")
}

// TestConcurrentCreateDatabase: N concurrent CreateDatabase of distinct names all commit
// through the real ddLock with no lost updates; view ≡ persisted truth.
func TestConcurrentCreateDatabase(t *testing.T) {
	meta := remoteMeta(t)

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = meta.CreateDatabase(context.Background(), dbModel(int64(2000+i), fmt.Sprintf("db_%d", i)), uint64(200+i))
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		require.NoError(t, e, "db_%d", i)
	}

	truth := persistedDBNames(t)
	for i := 0; i < n; i++ {
		require.Contains(t, truth, fmt.Sprintf("db_%d", i))
	}
}
