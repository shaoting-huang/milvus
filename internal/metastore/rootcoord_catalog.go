package metastore

import (
	pkgmetastore "github.com/milvus-io/milvus/pkg/v3/metastore"
)

// RootCoordCatalog + AlterType were moved to the shared pkg/v3/metastore module
// (so the pooled catalog service can depend on the rootcoord contract); they are
// re-exported here to keep the internal import path working unchanged. The other
// coord *Catalog contracts stay defined in their own per-coord files until each
// coord's own move.
type (
	RootCoordCatalog = pkgmetastore.RootCoordCatalog
	AlterType        = pkgmetastore.AlterType
)

const (
	ADD    = pkgmetastore.ADD
	DELETE = pkgmetastore.DELETE
	MODIFY = pkgmetastore.MODIFY
)
