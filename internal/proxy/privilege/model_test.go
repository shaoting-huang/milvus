package privilege

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBMatchFunc_NameBased(t *testing.T) {
	result, err := DBMatchFunc("Collection-default.col1", "Collection-default.col2")
	assert.NoError(t, err)
	assert.Equal(t, true, result)

	result, err = DBMatchFunc("Collection-db1.col1", "Collection-db2.col2")
	assert.NoError(t, err)
	assert.Equal(t, false, result)
}

func TestDBMatchFunc_IDBased(t *testing.T) {
	// Both ID-based, same → match
	result, err := DBMatchFunc("Collection-ID:12345", "Collection-ID:12345")
	assert.NoError(t, err)
	assert.Equal(t, true, result)

	// Both ID-based, different → no match
	result, err = DBMatchFunc("Collection-ID:12345", "Collection-ID:99999")
	assert.NoError(t, err)
	assert.Equal(t, false, result)

	// Mixed: one ID-based, one name-based → no match
	result, err = DBMatchFunc("Collection-ID:12345", "Collection-default.col1")
	assert.NoError(t, err)
	assert.Equal(t, false, result)

	result, err = DBMatchFunc("Collection-default.col1", "Collection-ID:12345")
	assert.NoError(t, err)
	assert.Equal(t, false, result)
}

func TestCollMatch_NameBased(t *testing.T) {
	assert.True(t, collMatch("Collection-default.col1", "Collection-default.col1"))
	assert.True(t, collMatch("Collection-default.*", "Collection-default.col1"))
	assert.True(t, collMatch("Collection-default.col1", "Collection-default.*"))
	assert.False(t, collMatch("Collection-default.col1", "Collection-default.col2"))
}

func TestCollMatch_IDBased(t *testing.T) {
	// Both ID-based, same → match
	assert.True(t, collMatch("Collection-ID:12345", "Collection-ID:12345"))

	// Both ID-based, different → no match
	assert.False(t, collMatch("Collection-ID:12345", "Collection-ID:99999"))

	// Mixed → no match
	assert.False(t, collMatch("Collection-ID:12345", "Collection-default.col1"))
	assert.False(t, collMatch("Collection-default.col1", "Collection-ID:12345"))

	// ID-based with wildcard name-based → no match (wildcard only works for name-based)
	assert.False(t, collMatch("Collection-ID:12345", "Collection-default.*"))
}
