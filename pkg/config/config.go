package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config 全局配置（包含服务器与客户端两部分）
type Config struct {
	Server ServerConfig
	Client ClientConfig
}

// ServerConfig 服务器配置项
type ServerConfig struct {
	NodeID           int    // 节点ID（由启动参数设置）
	ClientPortBase   int    // 客户端监听端口起始值
	PeerPortBase     int    // Raft对等端口起始值
	NodeCount        int    // 节点数量
	Host             string // 主机名或IP
	MaxWALEntries    int    // 单个WAL文件的最大条目数
	SnapshotInterval int    // 快照间隔（按已应用日志条目计）
	DataBasePath     string // 数据根目录
	WALSubdir        string // WAL子目录名
	RaftStateFile    string // Raft状态文件名
	SnapshotFile     string // 快照文件名
}

// ClientConfig 客户端配置项
type ClientConfig struct {
	Servers     []string // 服务器地址列表
	HistoryDir  string   // 客户端历史记录目录
	HistoryFile string   // 客户端历史记录文件名
}

// LoadConfig 从指定路径加载 .env 配置（若存在），并填充默认值
func LoadConfig(configPath string) (*Config, error) {
	// 如果配置文件存在，则加载它
	if _, err := os.Stat(configPath); err == nil {
		if err := godotenv.Load(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config file: %v", err)
		}
	}

	config := &Config{}

	// 服务器配置
	config.Server = ServerConfig{
		NodeID:           getEnvInt("SERVER_NODE_ID", 0),
		ClientPortBase:   getEnvInt("SERVER_CLIENT_PORT_BASE", 9000),
		PeerPortBase:     getEnvInt("SERVER_PEER_PORT_BASE", 8000),
		NodeCount:        getEnvInt("SERVER_NODE_COUNT", 3),
		Host:             getEnvString("SERVER_HOST", "localhost"),
		MaxWALEntries:    getEnvInt("SERVER_MAX_WAL_ENTRIES", 1000),
		SnapshotInterval: getEnvInt("SERVER_SNAPSHOT_INTERVAL", 1000),
		DataBasePath:     getEnvString("SERVER_DATA_BASE_PATH", "data"),
		WALSubdir:        getEnvString("SERVER_WAL_SUBDIR", "wal"),
		RaftStateFile:    getEnvString("SERVER_RAFT_STATE_FILE", "raft-state.pb"),
		SnapshotFile:     getEnvString("SERVER_SNAPSHOT_FILE", "snapshot.pb"),
	}

	// 客户端配置
	serversStr := getEnvString("CLIENT_SERVERS", "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002")
	config.Client = ClientConfig{
		Servers:     strings.Split(serversStr, ","),
		HistoryDir:  getEnvString("CLIENT_HISTORY_DIR", "history"),
		HistoryFile: getEnvString("CLIENT_HISTORY_FILE", "kvcli_history"),
	}

	return config, nil
}

// GetServerConfig 获取某个节点的服务器配置（返回副本，并设置节点ID）
func (c *Config) GetServerConfig(nodeID int) *ServerConfig {
	serverConfig := c.Server
	serverConfig.NodeID = nodeID
	return &serverConfig
}

// GetPeerAddrs 返回所有节点的对等端口地址映射（id -> addr）
func (c *Config) GetPeerAddrs() map[int]string {
	peerAddrs := make(map[int]string)
	for i := 0; i < c.Server.NodeCount; i++ {
		peerAddrs[i] = fmt.Sprintf("%s:%d", c.Server.Host, c.Server.PeerPortBase+i)
	}
	return peerAddrs
}

// GetClientAddr 返回指定节点的客户端监听地址
func (c *Config) GetClientAddr(nodeID int) string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.ClientPortBase+nodeID)
}

// GetDataPath 返回指定节点的数据目录
func (c *Config) GetDataPath(nodeID int) string {
	return filepath.Join(c.Server.DataBasePath, fmt.Sprintf("node%d", nodeID))
}

// GetWALDir 返回指定节点的WAL目录
func (c *Config) GetWALDir(nodeID int) string {
	return filepath.Join(c.GetDataPath(nodeID), c.Server.WALSubdir)
}

// GetRaftStatePath 返回指定节点的Raft状态文件路径
func (c *Config) GetRaftStatePath(nodeID int) string {
	return filepath.Join(c.GetDataPath(nodeID), c.Server.RaftStateFile)
}

// GetSnapshotPath 返回指定节点的快照文件路径
func (c *Config) GetSnapshotPath(nodeID int) string {
	return filepath.Join(c.GetDataPath(nodeID), c.Server.SnapshotFile)
}

// ===== 环境变量读取辅助 =====
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
