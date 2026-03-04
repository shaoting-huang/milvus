package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func Test_GetPrivilegeExtObj(t *testing.T) {
	request := &milvuspb.LoadCollectionRequest{
		DbName:         "test",
		CollectionName: "col1",
	}
	privilegeExt, err := GetPrivilegeExtObj(request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ObjectType_Collection, privilegeExt.ObjectType)
	assert.Equal(t, commonpb.ObjectPrivilege_PrivilegeLoad, privilegeExt.ObjectPrivilege)
	assert.Equal(t, int32(3), privilegeExt.ObjectNameIndex)

	request2 := &milvuspb.GetPersistentSegmentInfoRequest{}
	_, err = GetPrivilegeExtObj(request2)
	assert.Error(t, err)
}

func Test_GetResourceName(t *testing.T) {
	{
		request := &milvuspb.HasCollectionRequest{
			DbName:         "test",
			CollectionName: "col1",
		}
		assert.Equal(t, "*", GetObjectName(request, 0))
		assert.Equal(t, "col1", GetObjectName(request, 3))
	}

	{
		request := &milvuspb.SelectUserRequest{
			User: &milvuspb.UserEntity{Name: "test"},
		}
		assert.Equal(t, "test", GetObjectName(request, 2))

		request = &milvuspb.SelectUserRequest{}
		assert.Equal(t, "*", GetObjectName(request, 2))
	}
}

func Test_GetResourceNames(t *testing.T) {
	request := &milvuspb.FlushRequest{
		DbName:          "test",
		CollectionNames: []string{"col1", "col2"},
	}
	assert.Equal(t, 0, len(GetObjectNames(request, 0)))
	assert.Equal(t, 0, len(GetObjectNames(request, 2)))
	names := GetObjectNames(request, 3)
	assert.Equal(t, 2, len(names))
}

func Test_PolicyForPrivilege(t *testing.T) {
	assert.Equal(t,
		`{"PType":"p","V0":"admin","V1":"COLLECTION-default.col1","V2":"ALL"}`,
		PolicyForPrivilege("admin", "COLLECTION", "col1", "ALL", "default"))
}

func Test_PolicyForResource(t *testing.T) {
	assert.Equal(t,
		`COLLECTION-default.col1`,
		PolicyForResource("", "COLLECTION", "col1"))

	assert.Equal(t,
		`COLLECTION-db.col1`,
		PolicyForResource("db", "COLLECTION", "col1"))
}

func Test_PolicyCheckerWithRole(t *testing.T) {
	a := PolicyForPrivilege("admin", "COLLECTION", "col1", "ALL", "default")
	b := PolicyForPrivilege("foo", "COLLECTION", "col1", "ALL", "default")
	assert.True(t, PolicyCheckerWithRole(a, "admin"))
	assert.False(t, PolicyCheckerWithRole(b, "admin"))
}

func Test_PolicyForResourceByID(t *testing.T) {
	assert.Equal(t, "Collection-ID:12345", PolicyForResourceByID("Collection", 12345))
	assert.Equal(t, "Database-ID:99", PolicyForResourceByID("Database", 99))
}

func Test_IsIDBasedResource(t *testing.T) {
	assert.True(t, IsIDBasedResource("Collection-ID:12345"))
	assert.True(t, IsIDBasedResource("Database-ID:99"))
	assert.False(t, IsIDBasedResource("Collection-default.col1"))
	assert.False(t, IsIDBasedResource("Global-*"))
}

func Test_ParseEntityIDFromResource(t *testing.T) {
	assert.Equal(t, int64(12345), ParseEntityIDFromResource("Collection-ID:12345"))
	assert.Equal(t, int64(-1), ParseEntityIDFromResource("Collection-default.col1"))
	assert.Equal(t, int64(-1), ParseEntityIDFromResource("invalid"))
}

func Test_EntityIDObjectName(t *testing.T) {
	// Format
	assert.Equal(t, "ID:12345", FormatEntityIDObjectName(12345))

	// IsEntityIDObjectName
	assert.True(t, IsEntityIDObjectName("ID:12345"))
	assert.False(t, IsEntityIDObjectName("col1"))
	assert.False(t, IsEntityIDObjectName(""))

	// Parse
	assert.Equal(t, int64(12345), ParseEntityIDFromObjectName("ID:12345"))
	assert.Equal(t, int64(-1), ParseEntityIDFromObjectName("col1"))
	assert.Equal(t, int64(-1), ParseEntityIDFromObjectName("ID:abc"))
}

func Test_PolicyForPrivilegeV2(t *testing.T) {
	assert.Equal(t,
		`{"PType":"p","V0":"admin","V1":"Collection-ID:12345","V2":"Search"}`,
		PolicyForPrivilegeV2("admin", "Collection", 12345, "Search"))
}

func Test_PolicyForPrivilege_IDBasedObjectName(t *testing.T) {
	// When ObjectName is "ID:12345", PolicyForPrivilege should use ID-based resource format.
	result := PolicyForPrivilege("admin", "Collection", "ID:12345", "Search", "default")
	assert.Equal(t,
		`{"PType":"p","V0":"admin","V1":"Collection-ID:12345","V2":"Search"}`,
		result)

	// Normal name-based ObjectName should still work.
	result = PolicyForPrivilege("admin", "Collection", "col1", "Search", "default")
	assert.Equal(t,
		`{"PType":"p","V0":"admin","V1":"Collection-default.col1","V2":"Search"}`,
		result)
}
