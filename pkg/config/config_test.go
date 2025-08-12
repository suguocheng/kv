package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// 保存原始环境变量
	originalMaxWAL := os.Getenv("SERVER_MAX_WAL_ENTRIES")
	originalSnapshot := os.Getenv("SERVER_SNAPSHOT_INTERVAL")
	originalPort0 := os.Getenv("SERVER_PORT_0")
	originalPort1 := os.Getenv("SERVER_PORT_1")
	originalPort2 := os.Getenv("SERVER_PORT_2")

	// 设置测试环境变量
	os.Setenv("SERVER_MAX_WAL_ENTRIES", "1000")
	os.Setenv("SERVER_SNAPSHOT_INTERVAL", "100")
	os.Setenv("SERVER_PORT_0", "8000")
	os.Setenv("SERVER_PORT_1", "8001")
	os.Setenv("SERVER_PORT_2", "8002")

	// 恢复原始环境变量
	defer func() {
		os.Setenv("SERVER_MAX_WAL_ENTRIES", originalMaxWAL)
		os.Setenv("SERVER_SNAPSHOT_INTERVAL", originalSnapshot)
		os.Setenv("SERVER_PORT_0", originalPort0)
		os.Setenv("SERVER_PORT_1", originalPort1)
		os.Setenv("SERVER_PORT_2", originalPort2)
	}()

	// 测试加载配置
	cfg := LoadConfig()

	// 验证配置
	if cfg.MaxWALEntries != 1000 {
		t.Errorf("Expected MaxWALEntries=1000, got %d", cfg.MaxWALEntries)
	}

	if cfg.SnapshotInterval != 100 {
		t.Errorf("Expected SnapshotInterval=100, got %d", cfg.SnapshotInterval)
	}

	if len(cfg.ServerPorts) != 3 {
		t.Errorf("Expected 3 server ports, got %d", len(cfg.ServerPorts))
	}
}

func TestGetServerConfig(t *testing.T) {
	cfg := &Config{
		MaxWALEntries:    1000,
		SnapshotInterval: 100,
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
	cfg := &Config{}

	addr := cfg.GetClientAddr(0)
	expected := "localhost:9000"
	if addr != expected {
		t.Errorf("Expected %s, got %s", expected, addr)
	}

	addr = cfg.GetClientAddr(1)
	expected = "localhost:9001"
	if addr != expected {
		t.Errorf("Expected %s, got %s", expected, addr)
	}
}

func TestGetPeerAddrs(t *testing.T) {
	cfg := &Config{}

	addrs := cfg.GetPeerAddrs()
	if len(addrs) != 3 {
		t.Errorf("Expected 3 peer addresses, got %d", len(addrs))
	}

	expected := map[int]string{
		0: "localhost:8000",
		1: "localhost:8001",
		2: "localhost:8002",
	}

	for i, expectedAddr := range expected {
		if addrs[i] != expectedAddr {
			t.Errorf("Expected peer %d to be %s, got %s", i, expectedAddr, addrs[i])
		}
	}
}

func TestGetWALDir(t *testing.T) {
	cfg := &Config{}

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
	cfg := &Config{}

	path := cfg.GetRaftStatePath(0)
	expected := "data/node0/raft-state.pb"
	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}

func TestGetSnapshotPath(t *testing.T) {
	cfg := &Config{}

	path := cfg.GetSnapshotPath(0)
	expected := "data/node0/snapshot.pb"
	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}
