package rootcoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
)

// catalogMigratedMarker is a durable key in this cluster's OWN source backend recording that
// its metadata has been migrated into the catalog service. It makes convergence idempotent
// across restarts: once set, the coord just routes to the service instead of re-migrating.
const catalogMigratedMarker = "root-coord/_catalog_migrated"

// ConvergeCatalog drives this cluster to the desired state of "served by the catalog service",
// the action behind flipping rootCoord.catalogService.enabled at runtime:
//
//   - not yet migrated -> Migrate (gate -> bulk-import -> verify -> cut over). On success it
//     stamps the marker; on a verify failure Migrate has already rolled back to the source and
//     the marker stays unset, so the next tick retries.
//   - already migrated -> just cut over to the service-backed meta (no re-migration).
func ConvergeCatalog(ctx context.Context, cfg MigrationConfig) error {
	migrated, err := cfg.SourceKV.Has(ctx, catalogMigratedMarker)
	if err != nil {
		return err
	}
	if migrated {
		remote, err := cfg.BuildRemote()
		if err != nil {
			return err
		}
		cfg.Switchable.Switch(remote)
		log.Ctx(ctx).Info("catalog already migrated; routing to the service", zap.String("namespace", cfg.Namespace))
		return nil
	}

	if err := Migrate(ctx, cfg); err != nil {
		return err
	}
	// Cutover already happened inside Migrate. The marker is best-effort idempotency: if the
	// write fails, don't undo the cutover — just warn. (A restart would re-run the idempotent
	// migration, costing only a brief write-block window, not correctness.)
	if err := cfg.SourceKV.Save(ctx, catalogMigratedMarker, "1"); err != nil {
		log.Ctx(ctx).Warn("catalog migrated and cut over, but failed to stamp the migrated marker",
			zap.String("namespace", cfg.Namespace), zap.Error(err))
		return nil
	}
	log.Ctx(ctx).Info("catalog migrated and cut over to the service", zap.String("namespace", cfg.Namespace))
	return nil
}
