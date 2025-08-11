package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// 创建临时配置文件
	content := `
# 服务器配置
NODES=3
CLIENT_PORT_BASE=8000
PEER_PORT_BASE=9000

# 节点配置
NODE0_CLIENT_ADDR=localhost:8000
NODE0_PEER_ADDR=localhost:9000
NODE0_DATA_DIR=data/node0

NODE1_CLIENT_ADDR=localhost:8001
NODE1_PEER_ADDR=localhost:9001
NODE1_DATA_DIR=data/node1

NODE2_CLIENT_ADDR=localhost:8002
NODE2_PEER_ADDR=localhost:9002
NODE2_DATA_DIR=data/node2

# 客户端配置
CLIENT_SERVERS=localhost:8000,localhost:8001,localhost:8002
CLIENT_HISTORY_DIR=history
CLIENT_HISTORY_FILE=kvcli_history

# 服务器配置
SERVER_MAX_WAL_ENTRIES=1000
SERVER_SNAPSHOT_INTERVAL=100
`

	tmpfile, err := os.CreateTemp("", "config_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	// 测试加载配置
	cfg, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// 验证配置
	if cfg.Server.NodeCount != 3 {
		t.Errorf("Expected NodeCount=3, got %d", cfg.Server.NodeCount)
	}

	if cfg.Server.ClientPortBase != 9000 {
		t.Errorf("Expected ClientPortBase=9000, got %d", cfg.Server.ClientPortBase)
	}

	if len(cfg.Client.Servers) != 3 {
		t.Errorf("Expected 3 client servers, got %d", len(cfg.Client.Servers))
	}
}

func TestGetServerConfig(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			MaxWALEntries:    1000,
			SnapshotInterval: 100,
		},
	}

	serverCfg := cfg.GetServerConfig(0)
	if serverCfg.MaxWALEntries != 1000 {
		t.Errorf("Expected MaxWALEntries=1000, got %d", serverCfg.MaxWALEntries)
	}

	if serverCfg.SnapshotInterval != 100 {
		t.Errorf("Expected SnapshotInterval=100, got %d", serverCfg.SnapshotInterval)
	}
}

func TestGetClientAddr(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host:           "localhost",
			ClientPortBase: 8000,
		},
	}

	addr := cfg.GetClientAddr(0)
	expected := "localhost:8000"
	if addr != expected {
		t.Errorf("Expected %s, got %s", expected, addr)
	}

	addr = cfg.GetClientAddr(1)
	expected = "localhost:8001"
	if addr != expected {
		t.Errorf("Expected %s, got %s", expected, addr)
	}
}

func TestGetPeerAddrs(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			NodeCount:    3,
			Host:         "localhost",
			PeerPortBase: 9000,
		},
	}

	addrs := cfg.GetPeerAddrs()
	if len(addrs) != 3 {
		t.Errorf("Expected 3 peer addresses, got %d", len(addrs))
	}

	expected := map[int]string{
		0: "localhost:9000",
		1: "localhost:9001",
		2: "localhost:9002",
	}

	for i, expectedAddr := range expected {
		if addrs[i] != expectedAddr {
			t.Errorf("Expected peer %d to be %s, got %s", i, expectedAddr, addrs[i])
		}
	}
}

func TestGetWALDir(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			NodeCount:    3,
			DataBasePath: "data",
			WALSubdir:    "wal",
		},
	}

	walDir := cfg.GetWALDir(0)
	expected := "data/node0/wal"
	if walDir != expected {
		t.Errorf("Expected %s, got %s", expected, walDir)
	}

	walDir = cfg.GetWALDir(1)
	expected = "data/node1/wal"
	if walDir != expected {
		t.Errorf("Expected %s, got %s", expected, walDir)
	}
}

func TestGetRaftStatePath(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			NodeCount:     3,
			DataBasePath:  "data",
			RaftStateFile: "raft-state.pb",
		},
	}

	path := cfg.GetRaftStatePath(0)
	expected := "data/node0/raft-state.pb"
	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}

func TestGetSnapshotPath(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			NodeCount:    3,
			DataBasePath: "data",
			SnapshotFile: "snapshot.pb",
		},
	}

	path := cfg.GetSnapshotPath(0)
	expected := "data/node0/snapshot.pb"
	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}
