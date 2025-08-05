package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config 全局配置结构
type Config struct {
	Server ServerConfig
	Client ClientConfig
}

// ServerConfig 服务器配置
type ServerConfig struct {
	NodeID           int
	ClientPortBase   int
	PeerPortBase     int
	NodeCount        int
	Host             string
	MaxWALEntries    int
	SnapshotInterval int
	DataBasePath     string
	WALSubdir        string
	RaftStateFile    string
	SnapshotFile     string
}

// ClientConfig 客户端配置
type ClientConfig struct {
	Servers     []string
	HistoryDir  string
	HistoryFile string
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	// 如果配置文件存在，则加载它
	if _, err := os.Stat(configPath); err == nil {
		if err := godotenv.Load(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config file: %v", err)
		}
	}

	config := &Config{}

	// 加载服务器配置
	config.Server = ServerConfig{
		NodeID:           getEnvInt("SERVER_NODE_ID", 0),
		ClientPortBase:   getEnvInt("SERVER_CLIENT_PORT_BASE", 9000),
		PeerPortBase:     getEnvInt("SERVER_PEER_PORT_BASE", 8000),
		NodeCount:        getEnvInt("SERVER_NODE_COUNT", 3),
		Host:             getEnvString("SERVER_HOST", "localhost"),
		MaxWALEntries:    getEnvInt("SERVER_MAX_WAL_ENTRIES", 5),
		SnapshotInterval: getEnvInt("SERVER_SNAPSHOT_INTERVAL", 5),
		DataBasePath:     getEnvString("SERVER_DATA_BASE_PATH", "data"),
		WALSubdir:        getEnvString("SERVER_WAL_SUBDIR", "wal"),
		RaftStateFile:    getEnvString("SERVER_RAFT_STATE_FILE", "raft-state.pb"),
		SnapshotFile:     getEnvString("SERVER_SNAPSHOT_FILE", "snapshot.pb"),
	}

	// 加载客户端配置
	serversStr := getEnvString("CLIENT_SERVERS", "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002")
	config.Client = ClientConfig{
		Servers:     strings.Split(serversStr, ","),
		HistoryDir:  getEnvString("CLIENT_HISTORY_DIR", "history"),
		HistoryFile: getEnvString("CLIENT_HISTORY_FILE", "kvcli_history"),
	}

	return config, nil
}

// GetServerConfig 获取服务器配置
func (c *Config) GetServerConfig(nodeID int) *ServerConfig {
	// 创建节点特定的配置副本
	serverConfig := c.Server
	serverConfig.NodeID = nodeID
	return &serverConfig
}

// GetPeerAddrs 获取所有节点的地址映射
func (c *Config) GetPeerAddrs() map[int]string {
	peerAddrs := make(map[int]string)
	for i := 0; i < c.Server.NodeCount; i++ {
		peerAddrs[i] = fmt.Sprintf("%s:%d", c.Server.Host, c.Server.PeerPortBase+i)
	}
	return peerAddrs
}

// GetClientAddr 获取指定节点的客户端地址
func (c *Config) GetClientAddr(nodeID int) string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.ClientPortBase+nodeID)
}

// GetDataPath 获取指定节点的数据路径
func (c *Config) GetDataPath(nodeID int) string {
	return filepath.Join(c.Server.DataBasePath, fmt.Sprintf("node%d", nodeID))
}

// GetWALDir 获取指定节点的WAL目录
func (c *Config) GetWALDir(nodeID int) string {
	return filepath.Join(c.GetDataPath(nodeID), c.Server.WALSubdir)
}

// GetRaftStatePath 获取指定节点的Raft状态文件路径
func (c *Config) GetRaftStatePath(nodeID int) string {
	return filepath.Join(c.GetDataPath(nodeID), c.Server.RaftStateFile)
}

// GetSnapshotPath 获取指定节点的快照文件路径
func (c *Config) GetSnapshotPath(nodeID int) string {
	return filepath.Join(c.GetDataPath(nodeID), c.Server.SnapshotFile)
}

// 辅助函数
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
