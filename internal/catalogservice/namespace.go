package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/catalogservice/nsmeta"
)

// DefaultNamespace is used when a caller does not stamp a namespace (single-tenant mode).
const DefaultNamespace = nsmeta.Default

// namespaceMetadataKey is the gRPC metadata key carrying the caller's namespace (cluster-id).
const namespaceMetadataKey = nsmeta.Key

// WithNamespace stamps the namespace on the outgoing gRPC context (coord-side client).
func WithNamespace(ctx context.Context, namespace string) context.Context {
	return nsmeta.With(ctx, namespace)
}

// NamespaceFromContext reads the namespace off the incoming gRPC context (service-side),
// falling back to DefaultNamespace when none was stamped.
func NamespaceFromContext(ctx context.Context) string {
	return nsmeta.From(ctx)
}
