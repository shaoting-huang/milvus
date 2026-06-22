package channel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// RecoverPChannelStatsManager must be idempotent: the pooled catalog service builds one
// MetaTable per namespace in a single process, and each reload() calls Recover. The first
// call sets the singleton; subsequent calls must merge their vchannels instead of panicking
// on a second Future.Set. Existing single-tenant tests Reset first, so they are unaffected.
func TestRecoverPChannelStatsManagerIdempotent(t *testing.T) {
	ResetStaticPChannelStatsManager()

	RecoverPChannelStatsManager([]string{"by-dev-rootcoord-dml_0_100v0"})
	mgr := StaticPChannelStatsManager.Get()

	require.NotPanics(t, func() {
		RecoverPChannelStatsManager([]string{"by-dev-rootcoord-dml_1_200v0"})
	}, "second Recover (different namespace) must not panic")

	// merge semantics: the singleton is preserved (not replaced) and stays usable.
	require.True(t, StaticPChannelStatsManager.Ready())
	require.Same(t, mgr, StaticPChannelStatsManager.Get(), "must keep the same manager and merge into it")
}
