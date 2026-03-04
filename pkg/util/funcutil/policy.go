package funcutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func GetVersion(m interface{}) (string, error) {
	log := log.Ctx(context.TODO())
	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetVersion failed", zap.Error(err))
		return "", err
	}
	if !proto.HasExtension(pbMsg.ProtoReflect().Descriptor().Options(), milvuspb.E_MilvusExtObj) {
		err := errors.New("Extension not found")
		log.Error("GetExtension fail", zap.Error(err))
		return "", err
	}
	extObj := proto.GetExtension(pbMsg.ProtoReflect().Descriptor().Options(), milvuspb.E_MilvusExtObj)
	version := extObj.(*milvuspb.MilvusExt).Version
	log.Debug("GetVersion success", zap.String("version", version))
	return version, nil
}

func GetPrivilegeExtObj(m interface{}) (commonpb.PrivilegeExt, error) {
	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetPrivilegeExtObj failed", zap.Error(err))
		return commonpb.PrivilegeExt{}, err
	}

	if !proto.HasExtension(pbMsg.ProtoReflect().Descriptor().Options(), commonpb.E_PrivilegeExtObj) {
		err := errors.New("Extension not found")
		log.RatedWarn(60, "GetPrivilegeExtObj failed", zap.Error(err))
		return commonpb.PrivilegeExt{}, err
	}
	extObj := proto.GetExtension(pbMsg.ProtoReflect().Descriptor().Options(), commonpb.E_PrivilegeExtObj)

	privilegeExt := extObj.(*commonpb.PrivilegeExt)
	log.RatedDebug(60, "GetPrivilegeExtObj success", zap.String("resource_type", privilegeExt.ObjectType.String()), zap.String("resource_privilege", privilegeExt.ObjectPrivilege.String()))
	return commonpb.PrivilegeExt{
		ObjectType:       privilegeExt.ObjectType,
		ObjectPrivilege:  privilegeExt.ObjectPrivilege,
		ObjectNameIndex:  privilegeExt.ObjectNameIndex,
		ObjectNameIndexs: privilegeExt.ObjectNameIndexs,
	}, nil
}

// GetObjectName get object name from the grpc message according to the field index. The field is a string.
func GetObjectName(m interface{}, index int32) string {
	if index <= 0 {
		return util.AnyWord
	}

	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetObjectName fail", zap.Error(err))
		return util.AnyWord
	}

	msgDesc := pbMsg.ProtoReflect().Descriptor()
	value := pbMsg.ProtoReflect().Get(msgDesc.Fields().ByNumber(protoreflect.FieldNumber(index)))
	user, ok := value.Interface().(protoreflect.Message)
	if ok {
		userDesc := user.Descriptor()
		value = user.Get(userDesc.Fields().ByNumber(protoreflect.FieldNumber(1)))
		if value.String() == "" {
			return util.AnyWord
		}
	}
	return value.String()
}

// GetObjectNames get object names from the grpc message according to the field index. The field is an array.
func GetObjectNames(m interface{}, index int32) []string {
	if index <= 0 {
		return []string{}
	}

	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetObjectNames fail", zap.Error(err))
		return []string{}
	}

	msgDesc := pbMsg.ProtoReflect().Descriptor()
	value := pbMsg.ProtoReflect().Get(msgDesc.Fields().ByNumber(protoreflect.FieldNumber(index)))
	names, ok := value.Interface().(protoreflect.List)
	if !ok {
		return []string{}
	}
	res := make([]string, names.Len())
	for i := 0; i < names.Len(); i++ {
		res[i] = names.Get(i).String()
	}
	return res
}

func PolicyForPrivilege(roleName string, objectType string, objectName string, privilege string, dbName string) string {
	var resource string
	if IsEntityIDObjectName(objectName) {
		entityID := ParseEntityIDFromObjectName(objectName)
		resource = PolicyForResourceByID(objectType, entityID)
	} else {
		resource = PolicyForResource(dbName, objectType, objectName)
	}
	return fmt.Sprintf(`{"PType":"p","V0":"%s","V1":"%s","V2":"%s"}`, roleName, resource, privilege)
}

func PolicyForPrivileges(grants []*milvuspb.GrantEntity) string {
	return strings.Join(lo.Map(grants, func(r *milvuspb.GrantEntity, _ int) string {
		return PolicyForPrivilege(r.Role.Name, r.Object.Name, r.ObjectName, r.Grantor.Privilege.Name, r.DbName)
	}), "|")
}

func PrivilegesForPolicy(policy string) []string {
	return strings.Split(policy, "|")
}

func PolicyForResource(dbName string, objectType string, objectName string) string {
	return fmt.Sprintf("%s-%s", objectType, CombineObjectName(dbName, objectName))
}

// PolicyForResourceByID builds an ID-based resource string for Casbin enforcement.
// Format: "ObjectType-ID:entityID", e.g. "Collection-ID:12345"
func PolicyForResourceByID(objectType string, entityID int64) string {
	return fmt.Sprintf("%s-ID:%d", objectType, entityID)
}

// PolicyForPrivilegeV2 builds a JSON policy string using entity ID.
func PolicyForPrivilegeV2(roleName string, objectType string, entityID int64, privilege string) string {
	return fmt.Sprintf(`{"PType":"p","V0":"%s","V1":"%s","V2":"%s"}`, roleName, PolicyForResourceByID(objectType, entityID), privilege)
}

// EntityIDObjectNamePrefix is the prefix for entity-ID-based object names.
const EntityIDObjectNamePrefix = "ID:"

// IsEntityIDObjectName returns true if the object name is an entity-ID-based name (e.g., "ID:12345").
func IsEntityIDObjectName(objectName string) bool {
	return strings.HasPrefix(objectName, EntityIDObjectNamePrefix)
}

// FormatEntityIDObjectName creates an entity-ID-based object name from an int64 ID.
func FormatEntityIDObjectName(entityID int64) string {
	return fmt.Sprintf("%s%d", EntityIDObjectNamePrefix, entityID)
}

// ParseEntityIDFromObjectName extracts the entity ID from an entity-ID-based object name.
// Returns -1 if parsing fails.
func ParseEntityIDFromObjectName(objectName string) int64 {
	if !IsEntityIDObjectName(objectName) {
		return -1
	}
	idStr := objectName[len(EntityIDObjectNamePrefix):]
	var id int64
	if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
		return -1
	}
	return id
}

// IsIDBasedResource returns true if the resource string uses ID-based format (e.g., "Collection-ID:12345").
// It checks that "-ID:" appears right after the object type prefix.
func IsIDBasedResource(resource string) bool {
	idx := strings.Index(resource, "-")
	if idx < 0 {
		return false
	}
	return strings.HasPrefix(resource[idx:], "-ID:")
}

// ParseEntityIDFromResource extracts the entity ID from an ID-based resource string.
// Returns -1 if parsing fails.
func ParseEntityIDFromResource(resource string) int64 {
	idx := strings.Index(resource, "-ID:")
	if idx < 0 {
		return -1
	}
	idStr := resource[idx+4:]
	var id int64
	if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
		return -1
	}
	return id
}

func CombineObjectName(dbName string, objectName string) string {
	if dbName == "" {
		dbName = util.DefaultDBName
	}
	return fmt.Sprintf("%s.%s", dbName, objectName)
}

func SplitObjectName(objectName string) (string, string) {
	if !strings.Contains(objectName, ".") {
		return util.DefaultDBName, objectName
	}
	names := strings.Split(objectName, ".")
	return names[0], names[1]
}

func PolicyCheckerWithRole(policy, roleName string) bool {
	return strings.Contains(policy, fmt.Sprintf(`"V0":"%s"`, roleName))
}
