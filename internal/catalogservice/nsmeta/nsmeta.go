// Package nsmeta carries the catalog-service namespace (cluster-id) on gRPC contexts. It is a
// leaf package (only grpc/metadata) so both the service side (catalogservice) and the coord
// side (rootcoord) can stamp/read the namespace without an import cycle.
package nsmeta

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// Key is the gRPC metadata key carrying the caller's namespace (cluster-id).
const Key = "x-milvus-catalog-namespace"

// Default is used when a caller does not stamp a namespace (single-tenant mode).
const Default = "_default"

// With stamps the namespace on the outgoing gRPC context (coord-side client).
func With(ctx context.Context, namespace string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, Key, namespace)
}

// From reads the namespace off the incoming gRPC context (service-side), falling back to
// Default when none was stamped.
func From(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Default
	}
	vals := md.Get(Key)
	if len(vals) == 0 || vals[0] == "" {
		return Default
	}
	return vals[0]
}
