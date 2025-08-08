package main

import (
	"fmt"
	"kv/log"
	"kv/pkg/config"
	"kv/pkg/kvstore"
	"kv/pkg/proto/kvpb"
	"kv/pkg/raft"
	"kv/pkg/server"
	"os"
	"strconv"

	"google.golang.org/protobuf/proto"
)

type NodeConfig struct {
	Me               int
	ClientAddr       string
	PeerAddrs        map[int]string
	RaftStatePath    string
	SnapshotPath     string
	WALDir           string
	MaxWALEntries    int // 每个WAL文件最大条目数
	SnapshotInterval int // 快照间隔
}

func parseNodeID() int {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <nodeID>")
		os.Exit(1)
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic("Invalid node ID")
	}
	return id
}

func loadNodeConfig(me int) *NodeConfig {
	// 加载配置文件
	cfg, err := config.LoadConfig("config.env")
	if err != nil {
		log.DPrintf("Failed to load config: %v", err)
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// 获取节点特定的配置
	serverConfig := cfg.GetServerConfig(me)
	peerAddrs := cfg.GetPeerAddrs()
	clientAddr := cfg.GetClientAddr(me)
	walDir := cfg.GetWALDir(me)
	raftStatePath := cfg.GetRaftStatePath(me)
	snapshotPath := cfg.GetSnapshotPath(me)

	return &NodeConfig{
		Me:               me,
		ClientAddr:       clientAddr,
		PeerAddrs:        peerAddrs,
		RaftStatePath:    raftStatePath,
		SnapshotPath:     snapshotPath,
		WALDir:           walDir,
		MaxWALEntries:    serverConfig.MaxWALEntries,
		SnapshotInterval: serverConfig.SnapshotInterval,
	}
}

func initKV(walDir string, maxEntries int) *kvstore.KV {
	kv, err := kvstore.NewKV(walDir, maxEntries)
	if err != nil {
		panic(err)
	}
	return kv
}

func initRaft(conf *NodeConfig, kv *kvstore.KV) (*raft.Raft, chan raft.ApplyMsg) {
	applyCh := make(chan raft.ApplyMsg)
	wm := kv.GetWALManager()
	rf := raft.Make(conf.Me, conf.PeerAddrs, conf.ClientAddr, applyCh, conf.RaftStatePath, conf.SnapshotPath, wm)

	go startApplyLoop(rf, kv, applyCh, conf.SnapshotInterval)

	return rf, applyCh
}

// 启动 Raft gRPC 服务端
func startRaftGRPCServer(rf *raft.Raft, addr string) {
	go func() {
		err := rf.ServeGRPC(addr)
		if err != nil {
			panic(err)
		}
	}()
}

func startApplyLoop(rf *raft.Raft, kv *kvstore.KV, applyCh chan raft.ApplyMsg, snapshotInterval int) {
	lastSnapshottedIndex := 0
	go func() {
		wm := kv.GetWALManager()
		for msg := range applyCh {
			// 2. 反序列化并apply到store
			if msg.CommandValid {
				var op kvpb.Op
				if err := proto.Unmarshal(msg.Command, &op); err == nil {
					switch op.Type {
					case "PUT":
						if op.Ttl > 0 {
							kv.PutWithTTL(op.Key, string(op.Value), op.Ttl)
						} else {
							kv.Put(op.Key, string(op.Value))
						}
					case "DELETE":
						kv.Delete(op.Key)
					case "Compact":
						revision, err := strconv.ParseInt(string(op.Value), 10, 64)
						if err != nil {
							fmt.Printf("Failed to parse compact revision: %v\n", err)
							continue
						}
						_, err = kv.Compact(revision)
						if err != nil {
							fmt.Printf("Failed to compact: %v\n", err)
						}
					case "Txn":
						var req kvpb.TxnRequest
						if err := proto.Unmarshal(op.Value, &req); err == nil {
							// 在raft层应用时，使用TxnWithoutWAL避免重复生成版本号
							if _, err := kv.TxnWithoutWAL(&req); err != nil {
								fmt.Printf("Failed to apply transaction: %v\n", err)
							}
						} else {
							fmt.Printf("Failed to decode txn value: %v\n", err)
						}
					default:
						fmt.Println("Unknown Op type:", op.Type)
					}

					if msg.CommandIndex%snapshotInterval == 0 && msg.CommandIndex > lastSnapshottedIndex {
						snapshot, _ := kv.SerializeState()
						rf.Snapshot(msg.CommandIndex, snapshot)
						lastSnapshottedIndex = msg.CommandIndex
						// 清理已快照的WAL文件
						if err := wm.CleanupWALFiles(uint64(msg.CommandIndex)); err != nil {
							log.DPrintf("Failed to cleanup WAL files: %v\n", err)
						} else {
							log.DPrintf("Cleaned up WAL files up to index %d\n", msg.CommandIndex)
						}
					}
				}
			} else if msg.SnapshotValid {
				ok := rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
				if ok {
					err := kv.RestoreFromSnapshotData(msg.Snapshot)
					if err != nil {
						log.DPrintf("Failed to restore state from snapshot: %v\n", err)
					} else {
						log.DPrintf("Restored state from snapshot at index %d\n", msg.SnapshotIndex)
					}
					lastSnapshottedIndex = msg.SnapshotIndex
					// 恢复后重放快照点后的WAL
					fmt.Printf("[BOOT] CondInstallSnapshot后，开始ReplayWALsFrom(%d)\n", msg.SnapshotIndex+1)
					kv.ReplayWALsFrom(uint64(msg.SnapshotIndex + 1))
				} else {
					log.DPrintf("CondInstallSnapshot rejected snapshot at index %d\n", msg.SnapshotIndex)
				}
			}
		}
	}()
}

func startClientListener(addr string, kv *kvstore.KV, rf *raft.Raft) {
	err := server.StartGRPCServer(addr, kv, rf)
	if err != nil {
		panic(err)
	}
}
