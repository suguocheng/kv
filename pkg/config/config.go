package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config 全局配置结构
type Config struct {
	Server ServerConfig
	Client ClientConfig

	// 新增配置项
	ServerPorts           []int
	MaxWALEntries         int
	SnapshotInterval      int
	ClientTimeout         time.Duration
	ClientRetryAttempts   int
	LogLevel              string
	LogFile               string
	MaxConcurrentRequests int
	RequestTimeout        time.Duration
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

// LoadConfig 加载配置
func LoadConfig() *Config {
	// 加载.env文件
	godotenv.Load("config.env")

	return &Config{
		ServerPorts:           getEnvAsSlice("SERVER_PORT_", []string{"9000", "9001", "9002"}),
		MaxWALEntries:         getEnvAsInt("SERVER_MAX_WAL_ENTRIES", 500),   // 减少默认值
		SnapshotInterval:      getEnvAsInt("SERVER_SNAPSHOT_INTERVAL", 500), // 减少默认值
		ClientTimeout:         getEnvAsDuration("CLIENT_TIMEOUT", 5*time.Second),
		ClientRetryAttempts:   getEnvAsInt("CLIENT_RETRY_ATTEMPTS", 3),
		LogLevel:              getEnv("LOG_LEVEL", "INFO"),
		LogFile:               getEnv("LOG_FILE", "log/kv.log"),
		MaxConcurrentRequests: getEnvAsInt("MAX_CONCURRENT_REQUESTS", 1000),
		RequestTimeout:        getEnvAsDuration("REQUEST_TIMEOUT", 30*time.Second),
	}
}

// GetServerConfig 获取服务器配置
func (c *Config) GetServerConfig(nodeID int) *ServerConfig {
	return &ServerConfig{
		NodeID:           nodeID,
		ClientPortBase:   9000,
		PeerPortBase:     8000,
		NodeCount:        3,
		Host:             "localhost",
		MaxWALEntries:    c.MaxWALEntries,
		SnapshotInterval: c.SnapshotInterval,
		DataBasePath:     "data",
		WALSubdir:        "wal",
		RaftStateFile:    "raft-state.pb",
		SnapshotFile:     "snapshot.pb",
	}
}

// GetPeerAddrs 获取所有节点的地址映射
func (c *Config) GetPeerAddrs() map[int]string {
	peerAddrs := make(map[int]string)
	for i := 0; i < 3; i++ {
		peerAddrs[i] = fmt.Sprintf("localhost:%d", 8000+i)
	}
	return peerAddrs
}

// GetClientAddr 获取指定节点的客户端地址
func (c *Config) GetClientAddr(nodeID int) string {
	return fmt.Sprintf("localhost:%d", 9000+nodeID)
}

// GetDataPath 获取指定节点的数据路径
func (c *Config) GetDataPath(nodeID int) string {
	return fmt.Sprintf("data/node%d", nodeID)
}

// GetWALDir 获取指定节点的WAL目录
func (c *Config) GetWALDir(nodeID int) string {
	return fmt.Sprintf("data/node%d/wal", nodeID)
}

// GetRaftStatePath 获取指定节点的Raft状态文件路径
func (c *Config) GetRaftStatePath(nodeID int) string {
	return fmt.Sprintf("data/node%d/raft-state.pb", nodeID)
}

// GetSnapshotPath 获取指定节点的快照文件路径
func (c *Config) GetSnapshotPath(nodeID int) string {
	return fmt.Sprintf("data/node%d/snapshot.pb", nodeID)
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

func getEnvAsSlice(prefix string, defaultValue []string) []int {
	var result []int
	for i := 0; i < len(defaultValue); i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		if value := os.Getenv(key); value != "" {
			if intValue, err := strconv.Atoi(value); err == nil {
				result = append(result, intValue)
			}
		}
	}
	if len(result) == 0 {
		// 转换默认值为int切片
		for _, s := range defaultValue {
			if intValue, err := strconv.Atoi(s); err == nil {
				result = append(result, intValue)
			}
		}
	}
	return result
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
