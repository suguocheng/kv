package server

import (
	"context"
	"fmt"
	"kv/pkg/kvstore"
	"kv/pkg/proto/kvpb"
	"kv/pkg/raft"
	"kv/pkg/watch"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// KVServer 实现KVService接口
type KVServer struct {
	kv *kvstore.KV
	rf *raft.Raft
	kvpb.UnimplementedKVServiceServer
}

// NewKVServer 创建新的KV服务器
func NewKVServer(kv *kvstore.KV, rf *raft.Raft) *KVServer {
	return &KVServer{
		kv: kv,
		rf: rf,
	}
}

// StartGRPCServer 启动gRPC服务器
func StartGRPCServer(addr string, kv *kvstore.KV, rf *raft.Raft) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvServer := NewKVServer(kv, rf)
	kvpb.RegisterKVServiceServer(grpcServer, kvServer)

	fmt.Printf("gRPC server listening on %s\n", addr)
	return grpcServer.Serve(lis)
}

// Get 实现Get RPC
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, err := s.kv.Get(req.Key)
	if err != nil {
		return &kvpb.GetResponse{
			Exists: false,
			Error:  err.Error(),
		}, nil
	}
	return &kvpb.GetResponse{
		Value:  value,
		Exists: true,
	}, nil
}

// Put 实现Put RPC
func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	op := &kvpb.Op{Type: "PUT", Key: req.Key, Value: []byte(req.Value), Ttl: 0}
	data, err := proto.Marshal(op)
	if err != nil {
		return &kvpb.PutResponse{Error: "marshal failed"}, nil
	}

	_, _, isLeader := s.rf.Start(data)
	if !isLeader {
		return &kvpb.PutResponse{Error: "Not_Leader"}, nil
	}

	return &kvpb.PutResponse{Revision: 1}, nil // 简化处理，实际应该等待raft应用完成
}

// PutWithTTL 实现PutWithTTL RPC
func (s *KVServer) PutWithTTL(ctx context.Context, req *kvpb.PutWithTTLRequest) (*kvpb.PutResponse, error) {
	op := &kvpb.Op{Type: "PUT", Key: req.Key, Value: []byte(req.Value), Ttl: req.Ttl}
	data, err := proto.Marshal(op)
	if err != nil {
		return &kvpb.PutResponse{Error: "marshal failed"}, nil
	}

	_, _, isLeader := s.rf.Start(data)
	if !isLeader {
		return &kvpb.PutResponse{Error: "Not_Leader"}, nil
	}

	return &kvpb.PutResponse{Revision: 1}, nil
}

// Delete 实现Delete RPC
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	op := &kvpb.Op{Type: "DELETE", Key: req.Key, Value: []byte{}, Ttl: 0}
	data, err := proto.Marshal(op)
	if err != nil {
		return &kvpb.DeleteResponse{Error: "marshal failed"}, nil
	}

	_, _, isLeader := s.rf.Start(data)
	if !isLeader {
		return &kvpb.DeleteResponse{Error: "Not_Leader"}, nil
	}

	return &kvpb.DeleteResponse{Revision: 1}, nil
}

// Txn 实现Txn RPC
func (s *KVServer) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	txnBytes, _ := proto.Marshal(req)
	op := &kvpb.Op{
		Type:  "Txn",
		Key:   "txn",
		Value: txnBytes,
		Ttl:   1,
	}
	opData, err := proto.Marshal(op)
	if err != nil {
		return nil, status.Error(codes.Internal, "marshal failed")
	}

	index, _, isLeader := s.rf.Start(opData)
	if !isLeader {
		return nil, status.Error(codes.FailedPrecondition, "Not_Leader")
	}

	// 等待raft应用完成
	if !s.rf.WaitForIndex(index, 5*time.Second) {
		return nil, status.Error(codes.DeadlineExceeded, "timeout")
	}

	// 检查是否仍然是leader
	_, stillLeader := s.rf.GetState()
	if !stillLeader {
		return nil, status.Error(codes.FailedPrecondition, "Not_Leader")
	}

	// 获取事务结果
	resp, err := s.kv.TxnWithoutWAL(req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// GetWithRevision 实现GetWithRevision RPC
func (s *KVServer) GetWithRevision(ctx context.Context, req *kvpb.GetWithRevisionRequest) (*kvpb.GetWithRevisionResponse, error) {
	value, revision, err := s.kv.GetWithRevision(req.Key, req.Revision)
	if err != nil {
		return &kvpb.GetWithRevisionResponse{
			Exists: false,
			Error:  err.Error(),
		}, nil
	}
	return &kvpb.GetWithRevisionResponse{
		Value:    value,
		Revision: revision,
		Exists:   true,
	}, nil
}

// GetHistory 实现GetHistory RPC
func (s *KVServer) GetHistory(ctx context.Context, req *kvpb.GetHistoryRequest) (*kvpb.GetHistoryResponse, error) {
	history, err := s.kv.GetHistory(req.Key, req.Limit)
	if err != nil {
		return &kvpb.GetHistoryResponse{Error: err.Error()}, nil
	}

	items := make([]*kvpb.VersionedKVPair, len(history))
	for i, item := range history {
		items[i] = &kvpb.VersionedKVPair{
			Key:             item.Key,
			Value:           item.Value,
			Ttl:             item.TTL,
			CreatedRevision: item.CreatedRev,
			ModRevision:     item.ModRev,
			Version:         item.Version,
			Deleted:         item.Deleted,
		}
	}

	return &kvpb.GetHistoryResponse{Items: items}, nil
}

// Range 实现Range RPC
func (s *KVServer) Range(ctx context.Context, req *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	items, revision, err := s.kv.Range(req.Key, req.RangeEnd, req.Revision, req.Limit)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	kvs := make([]*kvpb.VersionedKVPair, len(items))
	for i, item := range items {
		kvs[i] = &kvpb.VersionedKVPair{
			Key:             item.Key,
			Value:           item.Value,
			Ttl:             item.TTL,
			CreatedRevision: item.CreatedRev,
			ModRevision:     item.ModRev,
			Version:         item.Version,
			Deleted:         item.Deleted,
		}
	}

	return &kvpb.RangeResponse{
		Kvs:      kvs,
		Revision: revision,
	}, nil
}

// Compact 实现Compact RPC
func (s *KVServer) Compact(ctx context.Context, req *kvpb.CompactRequest) (*kvpb.CompactResponse, error) {
	compactedRev, err := s.kv.Compact(req.Revision)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// 通过raft层执行压缩
	op := &kvpb.Op{Type: "Compact", Key: "", Value: []byte(fmt.Sprintf("%d", req.Revision)), Ttl: 0}
	data, err := proto.Marshal(op)
	if err != nil {
		return nil, status.Error(codes.Internal, "marshal failed")
	}

	_, _, isLeader := s.rf.Start(data)
	if !isLeader {
		return nil, status.Error(codes.FailedPrecondition, "Not_Leader")
	}

	return &kvpb.CompactResponse{Revision: compactedRev}, nil
}

// GetStats 实现GetStats RPC
func (s *KVServer) GetStats(ctx context.Context, req *kvpb.GetStatsRequest) (*kvpb.GetStatsResponse, error) {
	stats := s.kv.GetStats()

	statsMap := make(map[string]string)
	for key, value := range stats {
		statsMap[fmt.Sprintf("%v", key)] = fmt.Sprintf("%v", value)
	}

	return &kvpb.GetStatsResponse{Stats: statsMap}, nil
}

// Watch 实现Watch RPC (流式)
func (s *KVServer) Watch(req *kvpb.WatchRequest, stream kvpb.KVService_WatchServer) error {
	var watcher *watch.Watcher
	var err error

	// 只支持单个键监听
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key must be specified")
	}

	if req.WatcherId != "" {
		watcher, err = s.kv.WatchKeyWithID(req.WatcherId, req.Key)
	} else {
		watcher, err = s.kv.WatchKey(req.Key)
	}

	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// 监听事件并发送
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				// 通道已关闭
				return nil
			}
			if event == nil {
				// 跳过nil事件
				continue
			}
			// 直接发送WatchEvent
			watchEvent := &kvpb.WatchEvent{
				Type:      int32(event.Type),
				Key:       event.Key,
				Value:     event.Value,
				Revision:  event.Revision,
				Timestamp: event.Timestamp.Unix(),
				Ttl:       event.TTL,
			}

			if err := stream.Send(watchEvent); err != nil {
				watcher.Close()
				return err
			}

		case <-stream.Context().Done():
			watcher.Close()
			return nil

		default:
			// 检查watcher是否已关闭
			if watcher.IsClosed() {
				return status.Error(codes.Canceled, "watcher cancelled")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Unwatch 实现Unwatch RPC
func (s *KVServer) Unwatch(ctx context.Context, req *kvpb.UnwatchRequest) (*kvpb.UnwatchResponse, error) {
	err := s.kv.Unwatch(req.WatcherId)
	if err != nil {
		return &kvpb.UnwatchResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &kvpb.UnwatchResponse{Success: true}, nil
}

// GetWatchList 实现GetWatchList RPC
func (s *KVServer) GetWatchList(ctx context.Context, req *kvpb.GetWatchListRequest) (*kvpb.GetWatchListResponse, error) {
	watchers := s.kv.ListWatchers()

	var watcherInfos []*kvpb.WatcherInfo
	for _, watcher := range watchers {
		if !watcher.IsClosed() {
			watcherInfos = append(watcherInfos, &kvpb.WatcherInfo{
				Id:     watcher.ID,
				Target: watcher.Key,
			})
		}
	}

	return &kvpb.GetWatchListResponse{Watchers: watcherInfos}, nil
}
