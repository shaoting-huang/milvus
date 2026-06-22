package migration

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// seed writes the given logical key->value pairs into a MetaKv.
func seed(t *testing.T, ctx context.Context, m interface {
	Save(ctx context.Context, key, value string) error
}, kvs map[string]string,
) {
	for k, v := range kvs {
		require.NoError(t, m.Save(ctx, k, v))
	}
}

func TestCopyPrefixes_RoundTrip(t *testing.T) {
	ctx := context.Background()
	src := newSrcKV(t, "catalog-test/mig-cd-rt-src")
	dst := newDstKV(t, "catalog-test/mig-cd-rt-dst")

	want := map[string]string{
		"root-coord/collection/100":       "coll-100",
		"root-coord/collection/200":       "coll-200",
		"root-coord/database/db-info/1":   "db-1",
		"root-coord/credential/users/foo": "pwd-foo",
	}
	seed(t, ctx, src, want)

	copied, err := CopyPrefixes(ctx, src, dst, []string{"root-coord"})
	require.NoError(t, err)
	require.Equal(t, len(want), copied)

	// dst must yield the same logical key->value set as src.
	keys, vals, err := dst.LoadWithPrefix(ctx, "root-coord")
	require.NoError(t, err)
	require.Equal(t, len(want), len(keys))

	got := map[string]string{}
	for i, k := range keys {
		// strip dst rootPath using the same logic the impl uses, by re-loading from src
		got[k] = vals[i]
	}
	// Compare via Diff (the authoritative round-trip check): identical => empty.
	mismatches, err := DiffPrefixes(ctx, src, dst, []string{"root-coord"})
	require.NoError(t, err)
	require.Empty(t, mismatches, "after copy src and dst must be logically identical, got: %v", mismatches)
}

func TestDiffPrefixes_ValueDiffers(t *testing.T) {
	ctx := context.Background()
	src := newSrcKV(t, "catalog-test/mig-cd-vd-src")
	dst := newDstKV(t, "catalog-test/mig-cd-vd-dst")

	seed(t, ctx, src, map[string]string{
		"root-coord/collection/100": "coll-100",
		"root-coord/collection/200": "coll-200",
	})
	_, err := CopyPrefixes(ctx, src, dst, []string{"root-coord"})
	require.NoError(t, err)

	// mutate one key's value in dst (same logical key, different value)
	require.NoError(t, dst.Save(ctx, "root-coord/collection/100", "tampered"))

	mismatches, err := DiffPrefixes(ctx, src, dst, []string{"root-coord"})
	require.NoError(t, err)
	require.Len(t, mismatches, 1)
	require.Equal(t, "root-coord/collection/100", mismatches[0].Key)
	require.Equal(t, "value-differs", mismatches[0].Reason)
}

func TestDiffPrefixes_MissingBothSides(t *testing.T) {
	ctx := context.Background()
	src := newSrcKV(t, "catalog-test/mig-cd-ms-src")
	dst := newDstKV(t, "catalog-test/mig-cd-ms-dst")

	seed(t, ctx, src, map[string]string{
		"root-coord/collection/100":      "coll-100", // copied to both
		"root-coord/collection/only-src": "src-only",
	})
	_, err := CopyPrefixes(ctx, src, dst, []string{"root-coord"})
	require.NoError(t, err)

	// remove the src-only key from dst so it is only in src
	require.NoError(t, dst.Remove(ctx, "root-coord/collection/only-src"))
	// add a key only in dst
	require.NoError(t, dst.Save(ctx, "root-coord/collection/only-dst", "dst-only"))

	mismatches, err := DiffPrefixes(ctx, src, dst, []string{"root-coord"})
	require.NoError(t, err)

	byKey := map[string]string{}
	for _, m := range mismatches {
		byKey[m.Key] = m.Reason
	}
	require.Equal(t, "missing-in-dst", byKey["root-coord/collection/only-src"])
	require.Equal(t, "missing-in-src", byKey["root-coord/collection/only-dst"])
	require.Len(t, mismatches, 2)

	// sanity: results are stable/sorted for deterministic reporting
	keys := make([]string, 0, len(mismatches))
	for _, m := range mismatches {
		keys = append(keys, m.Key)
	}
	require.True(t, sort.StringsAreSorted(keys), "mismatches should be sorted by key for deterministic output")
}
