// Code generated by protoc-gen-go.
// source: tsync.proto
// DO NOT EDIT!

/*
Package tsync is a generated protocol buffer package.

It is generated from these files:
	tsync.proto

It has these top-level messages:
	Chunk
	Response
*/
package main

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Chunk struct {
	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *Chunk) Reset()                    { *m = Chunk{} }
func (m *Chunk) String() string            { return proto.CompactTextString(m) }
func (*Chunk) ProtoMessage()               {}
func (*Chunk) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Chunk) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type Response struct {
}

func (m *Response) Reset()                    { *m = Response{} }
func (m *Response) String() string            { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()               {}
func (*Response) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func init() {
	proto.RegisterType((*Chunk)(nil), "tsync.Chunk")
	proto.RegisterType((*Response)(nil), "tsync.Response")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for Sync service

type SyncClient interface {
	Sync(ctx context.Context, opts ...grpc.CallOption) (Sync_SyncClient, error)
}

type syncClient struct {
	cc *grpc.ClientConn
}

func NewSyncClient(cc *grpc.ClientConn) SyncClient {
	return &syncClient{cc}
}

func (c *syncClient) Sync(ctx context.Context, opts ...grpc.CallOption) (Sync_SyncClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Sync_serviceDesc.Streams[0], c.cc, "/tsync.Sync/Sync", opts...)
	if err != nil {
		return nil, err
	}
	x := &syncSyncClient{stream}
	return x, nil
}

type Sync_SyncClient interface {
	Send(*Chunk) error
	CloseAndRecv() (*Response, error)
	grpc.ClientStream
}

type syncSyncClient struct {
	grpc.ClientStream
}

func (x *syncSyncClient) Send(m *Chunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *syncSyncClient) CloseAndRecv() (*Response, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(Response)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for Sync service

type SyncServer interface {
	Sync(Sync_SyncServer) error
}

func RegisterSyncServer(s *grpc.Server, srv SyncServer) {
	s.RegisterService(&_Sync_serviceDesc, srv)
}

func _Sync_Sync_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SyncServer).Sync(&syncSyncServer{stream})
}

type Sync_SyncServer interface {
	SendAndClose(*Response) error
	Recv() (*Chunk, error)
	grpc.ServerStream
}

type syncSyncServer struct {
	grpc.ServerStream
}

func (x *syncSyncServer) SendAndClose(m *Response) error {
	return x.ServerStream.SendMsg(m)
}

func (x *syncSyncServer) Recv() (*Chunk, error) {
	m := new(Chunk)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _Sync_serviceDesc = grpc.ServiceDesc{
	ServiceName: "tsync.Sync",
	HandlerType: (*SyncServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Sync",
			Handler:       _Sync_Sync_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "tsync.proto",
}

func init() { proto.RegisterFile("tsync.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 111 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2e, 0x29, 0xae, 0xcc,
	0x4b, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x05, 0x73, 0x94, 0xa4, 0xb9, 0x58, 0x9d,
	0x33, 0x4a, 0xf3, 0xb2, 0x85, 0x84, 0xb8, 0x58, 0x52, 0x12, 0x4b, 0x12, 0x25, 0x18, 0x15, 0x18,
	0x35, 0x78, 0x82, 0xc0, 0x6c, 0x25, 0x2e, 0x2e, 0x8e, 0xa0, 0xd4, 0xe2, 0x82, 0xfc, 0xbc, 0xe2,
	0x54, 0x23, 0x43, 0x2e, 0x96, 0xe0, 0xca, 0xbc, 0x64, 0x21, 0x4d, 0x28, 0xcd, 0xa3, 0x07, 0x31,
	0x0d, 0xac, 0x5b, 0x8a, 0x1f, 0xca, 0x83, 0x29, 0x57, 0x62, 0xd0, 0x60, 0x4c, 0x62, 0x03, 0xdb,
	0x64, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0xf3, 0x80, 0xb5, 0x09, 0x78, 0x00, 0x00, 0x00,
}