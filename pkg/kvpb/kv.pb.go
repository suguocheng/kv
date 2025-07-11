// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v3.21.12
// source: pkg/kvpb/kv.proto

package kvpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type KVPair struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           string                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value         string                 `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Ttl           int64                  `protobuf:"varint,3,opt,name=ttl,proto3" json:"ttl,omitempty"` // TTL in seconds, 0 means no expiration
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *KVPair) Reset() {
	*x = KVPair{}
	mi := &file_pkg_kvpb_kv_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KVPair) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVPair) ProtoMessage() {}

func (x *KVPair) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_kvpb_kv_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVPair.ProtoReflect.Descriptor instead.
func (*KVPair) Descriptor() ([]byte, []int) {
	return file_pkg_kvpb_kv_proto_rawDescGZIP(), []int{0}
}

func (x *KVPair) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KVPair) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *KVPair) GetTtl() int64 {
	if x != nil {
		return x.Ttl
	}
	return 0
}

type KVStore struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Pairs         []*KVPair              `protobuf:"bytes,1,rep,name=pairs,proto3" json:"pairs,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *KVStore) Reset() {
	*x = KVStore{}
	mi := &file_pkg_kvpb_kv_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KVStore) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVStore) ProtoMessage() {}

func (x *KVStore) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_kvpb_kv_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVStore.ProtoReflect.Descriptor instead.
func (*KVStore) Descriptor() ([]byte, []int) {
	return file_pkg_kvpb_kv_proto_rawDescGZIP(), []int{1}
}

func (x *KVStore) GetPairs() []*KVPair {
	if x != nil {
		return x.Pairs
	}
	return nil
}

var File_pkg_kvpb_kv_proto protoreflect.FileDescriptor

const file_pkg_kvpb_kv_proto_rawDesc = "" +
	"\n" +
	"\x11pkg/kvpb/kv.proto\x12\x04kvpb\"B\n" +
	"\x06KVPair\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value\x12\x10\n" +
	"\x03ttl\x18\x03 \x01(\x03R\x03ttl\"-\n" +
	"\aKVStore\x12\"\n" +
	"\x05pairs\x18\x01 \x03(\v2\f.kvpb.KVPairR\x05pairsB\rZ\vkv/pkg/kvpbb\x06proto3"

var (
	file_pkg_kvpb_kv_proto_rawDescOnce sync.Once
	file_pkg_kvpb_kv_proto_rawDescData []byte
)

func file_pkg_kvpb_kv_proto_rawDescGZIP() []byte {
	file_pkg_kvpb_kv_proto_rawDescOnce.Do(func() {
		file_pkg_kvpb_kv_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pkg_kvpb_kv_proto_rawDesc), len(file_pkg_kvpb_kv_proto_rawDesc)))
	})
	return file_pkg_kvpb_kv_proto_rawDescData
}

var file_pkg_kvpb_kv_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_pkg_kvpb_kv_proto_goTypes = []any{
	(*KVPair)(nil),  // 0: kvpb.KVPair
	(*KVStore)(nil), // 1: kvpb.KVStore
}
var file_pkg_kvpb_kv_proto_depIdxs = []int32{
	0, // 0: kvpb.KVStore.pairs:type_name -> kvpb.KVPair
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_pkg_kvpb_kv_proto_init() }
func file_pkg_kvpb_kv_proto_init() {
	if File_pkg_kvpb_kv_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pkg_kvpb_kv_proto_rawDesc), len(file_pkg_kvpb_kv_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_kvpb_kv_proto_goTypes,
		DependencyIndexes: file_pkg_kvpb_kv_proto_depIdxs,
		MessageInfos:      file_pkg_kvpb_kv_proto_msgTypes,
	}.Build()
	File_pkg_kvpb_kv_proto = out.File
	file_pkg_kvpb_kv_proto_goTypes = nil
	file_pkg_kvpb_kv_proto_depIdxs = nil
}
