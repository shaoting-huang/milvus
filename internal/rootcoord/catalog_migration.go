package rootcoord

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/catalogservice/migration"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// bulkImportClient is the narrow slice of the catalog service used by migration. The full
// catalogpb.CatalogServiceClient satisfies it; tests use a 2-method fake. Crucially the
// coord ships logical KVs over this RPC — it never gets a TiKV/etcd handle for the
// destination; the backend stays behind the service.
type bulkImportClient interface {
	BulkImport(ctx context.Context, in *catalogpb.BulkImportRequest, opts ...grpc.CallOption) (*catalogpb.BulkImportResponse, error)
	VerifyImport(ctx context.Context, in *catalogpb.VerifyImportRequest, opts ...grpc.CallOption) (*catalogpb.VerifyImportResponse, error)
}

// MigrationConfig drives a runtime, in-place migration of this cluster's RootCoord metadata
// from its own source backend (SourceKV, e.g. etcd) into the pooled catalog service's
// backend, then cuts c.meta over to a service-backed meta — no restart, clean rollback.
// The coord only ever reads its own source; the destination is written/verified by the
// service via Client, so TiKV/etcd is never exposed to the coord.
type MigrationConfig struct {
	Switchable  *switchableMetaTable
	Source      IMetaTable // pre-migration meta; serves reads during the gate; rollback target
	SourceKV    kv.MetaKv  // this cluster's own source backend to read FROM
	Roots       []string   // key prefixes to migrate (e.g. ["root-coord"])
	Namespace   string     // cluster-id; selects the destination prefix inside the service
	Client      bulkImportClient
	BuildRemote func() (IMetaTable, error) // cutover target (service-backed); called on diff-clean

	afterCopyHook func() // test seam: invoked after import, before verify (nil in production)
}

// Migrate: gate writes -> drain in-flight -> bulk-import to the service -> verify -> cut over
// (or roll back). Any failure restores the switchable to Source so writes resume unchanged.
func Migrate(ctx context.Context, cfg MigrationConfig) error {
	cfg.Switchable.Switch(newBlockingMetaTable(cfg.Source))

	rollback := func(cause error) error {
		cfg.Switchable.Switch(cfg.Source)
		return cause
	}

	drainInFlightWrites(cfg.Source)

	// read OWN source snapshot and ship it to the service (service writes its TiKV).
	srcMap, err := migration.LoadLogical(ctx, cfg.SourceKV, cfg.Roots)
	if err != nil {
		return rollback(err)
	}
	imp, err := cfg.Client.BulkImport(ctx, &catalogpb.BulkImportRequest{Namespace: cfg.Namespace, Entries: toEntries(srcMap)})
	if err != nil {
		return rollback(err)
	}
	if err := merr.Error(imp.GetStatus()); err != nil {
		return rollback(err)
	}
	log.Ctx(ctx).Info("catalog migration imported", zap.Int64("keys", imp.GetImported()))

	if cfg.afterCopyHook != nil {
		cfg.afterCopyHook()
	}

	// verify on the SERVICE side against a fresh source snapshot (catches in-flight leakage).
	freshMap, err := migration.LoadLogical(ctx, cfg.SourceKV, cfg.Roots)
	if err != nil {
		return rollback(err)
	}
	ver, err := cfg.Client.VerifyImport(ctx, &catalogpb.VerifyImportRequest{
		Namespace: cfg.Namespace, Roots: cfg.Roots, Entries: toEntries(freshMap),
	})
	if err != nil {
		return rollback(err)
	}
	if err := merr.Error(ver.GetStatus()); err != nil {
		return rollback(err)
	}
	if len(ver.GetMismatches()) > 0 {
		return rollback(merr.WrapErrServiceInternalMsg("catalog migration verify found %d mismatches, rolled back", len(ver.GetMismatches())))
	}

	remote, err := cfg.BuildRemote()
	if err != nil {
		return rollback(err)
	}
	cfg.Switchable.Switch(remote)
	log.Ctx(ctx).Info("catalog migration cut over to the service backend")
	return nil
}

func toEntries(m map[string]string) []*catalogpb.KvEntry {
	out := make([]*catalogpb.KvEntry, 0, len(m))
	for k, v := range m {
		out = append(out, &catalogpb.KvEntry{Key: k, Value: []byte(v)})
	}
	return out
}

// drainInFlightWrites waits for any write that entered the source MetaTable before the gate
// to finish, by acquiring its ddLock (every write path holds it). New writes are already
// rejected by the gate, so once this returns no source write is in flight.
func drainInFlightWrites(source IMetaTable) {
	if mt, ok := source.(*MetaTable); ok {
		// barrier both write locks: DDL writes hold ddLock, RBAC writes hold permissionLock.
		mt.ddLock.Lock()
		mt.ddLock.Unlock() //nolint:staticcheck // intentional barrier: wait for in-flight writers, then release
		mt.permissionLock.Lock()
		mt.permissionLock.Unlock() //nolint:staticcheck // intentional barrier
	}
}
