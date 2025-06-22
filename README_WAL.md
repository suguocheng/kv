# 多WAL文件 + 快照清理功能

## 概述

本项目实现了基于Raft的分布式KV存储系统，支持多WAL（Write-Ahead Log）文件和快照清理功能。当Raft生成快照后，系统会自动删除对应的WAL文件，以节省存储空间。

## 功能特性

### 1. 多WAL文件管理
- **自动分割**: 当单个WAL文件达到最大条目数时，自动创建新的WAL文件
- **文件命名**: 使用 `wal_XXXXXX.log` 格式，其中XXXXXX是6位数字ID
- **目录结构**: 所有WAL文件存储在 `data/nodeX/wal/` 目录下
- **编号递增**: 删除WAL文件后，新建文件的编号会自动递增，避免重复

### 2. 快照覆盖点清理
- **智能清理**: 基于快照覆盖点逻辑，只有完全被快照覆盖的WAL文件才会被删除
- **安全清理**: 确保WAL文件中的所有操作都已经被快照状态表达出来才删除
- **索引跟踪**: 精确计算每个WAL文件的最高日志索引，与快照索引比较

### 3. 数据恢复保证
- **快照优先**: 启动时先从快照恢复基础状态
- **WAL重放**: 然后从WAL文件重放快照之后的操作
- **数据一致性**: 确保重启后数据不会丢失

### 4. 配置参数
- **MaxWALEntries**: 每个WAL文件最大条目数（建议与快照阈值一致）
- **快照阈值**: 每N个日志条目生成一次快照（建议与MaxWALEntries一致）

## 核心逻辑

### 快照覆盖点算法
```go
// 计算每个WAL文件的最高日志索引
currentIndex := 0
for _, walFile := range kv.walFiles {
    // 计算这个WAL文件的最高日志索引
    fileHighestIndex := currentIndex + walFile.entries - 1
    
    // 如果这个WAL文件的最高日志索引 <= 快照索引，则可以删除
    if fileHighestIndex <= snapshotIndex {
        filesToDelete = append(filesToDelete, walFile.path)
    } else {
        // 这个文件还有未快照的条目，需要保留
        filesToKeep = append(filesToKeep, walFile)
    }
    currentIndex += walFile.entries
}
```

### 为什么需要快照覆盖点？
1. **WAL文件包含PUT和DEL操作**：一个WAL文件可能包含5个操作，但最终状态可能只有2个键值对
2. **快照只包含最终状态**：快照包含的是某个时间点的完整键值对状态
3. **简单计数不准确**：不能简单地按条目数判断是否可以删除WAL文件

### 正确的删除逻辑
- 只有当WAL文件的**最高日志索引 ≤ 快照索引**时，该WAL文件才能被安全删除
- 这确保了WAL文件中的所有操作都已经被快照状态表达出来

## 目录结构

```
data/
├── node0/
│   ├── wal/
│   │   ├── wal_000000.log  # 前5个操作
│   │   ├── wal_000001.log  # 第6-10个操作
│   │   └── wal_000002.log  # 第11-15个操作
│   ├── raft-state.pb
│   └── snapshot.pb         # 包含最终键值对状态
├── node1/
│   └── ...
└── node2/
    └── ...
```

## 使用方法

### 启动服务器

```bash
# 启动节点0
go run cmd/server/main.go 0

# 启动节点1
go run cmd/server/main.go 1

# 启动节点2
go run cmd/server/main.go 2
```

### 测试WAL功能

```bash
# 测试修复后的WAL功能（包含PUT和DEL操作）
go run test/test_fixed_wal.go 0
```

## 核心组件

### KV存储 (`pkg/kvstore/kv.go`)

```go
type KV struct {
    walDir             string        // WAL目录路径
    walFiles           []*WALFile    // WAL文件列表
    currentWAL         *WALFile      // 当前活跃的WAL文件
    store              *SkipList     // 内存存储
    maxEntries         int           // 每个文件最大条目数
    snapshotPath       string        // 快照文件路径
    walSeq             int           // 下一个WAL文件编号
    lastSnapshotIndex  int           // 最后快照的索引
}
```

### 主要方法

- `NewKV(walDir, maxEntries)`: 创建新的KV存储
- `restoreFromSnapshot()`: 从快照恢复状态
- `replayAllWALs()`: 重放WAL文件
- `Put(key, value)`: 写入键值对并记录到WAL
- `Delete(key)`: 删除键值对并记录到WAL
- `CleanupWALFiles(snapshotIndex)`: 基于快照覆盖点清理WAL文件
- `GetWALStats()`: 获取WAL统计信息

## 工作流程

### 1. 启动恢复
1. 从快照文件恢复基础状态
2. 从WAL文件重放快照之后的操作
3. 创建新的WAL文件，编号递增

### 2. 写入操作
1. 检查当前WAL文件是否存在且未满
2. 如果不存在或已满，创建新的WAL文件
3. 写入操作到当前WAL文件
4. 更新内存存储

### 3. 快照生成
1. Raft每N个条目生成一次快照（N = MaxWALEntries）
2. 调用 `SerializeState()` 序列化当前状态
3. 调用 `CleanupWALFiles()` 基于快照覆盖点清理WAL文件

### 4. WAL清理
1. 计算每个WAL文件的最高日志索引
2. 删除最高日志索引 ≤ 快照索引的WAL文件
3. 保留包含未快照条目的WAL文件

## 配置说明

### 服务器配置 (`cmd/server/main.go`)

```go
type NodeConfig struct {
    Me            int
    ClientAddr    string
    PeerAddrs     map[int]string
    RaftStatePath string
    SnapshotPath  string
    WALDir        string        // WAL目录
    MaxWALEntries int           // 每个文件最大条目数
}
```

### 建议配置
- **WAL目录**: `data/nodeX/wal/`
- **最大条目数**: 5个条目/文件（与快照阈值一致）
- **快照阈值**: 5个条目（与MaxWALEntries一致）

## 测试结果示例

### 修复后的WAL测试
```
Testing fixed WAL functionality for node 0
WAL directory: data/node0/wal
Max entries per file: 5

Adding test data with PUT and DEL operations...
PUT key1 = value1 (index 1)
PUT key2 = value2 (index 2)
DEL key1 (index 3)
PUT key3 = value3 (index 4)
DEL key2 (index 5)

--- Generating snapshot at index 5 ---
Snapshot size: 140 bytes
Successfully cleaned up WAL files up to index 5
WAL files after cleanup: 0, Total entries: 0
Remaining WAL files: 

PUT key4 = value4 (index 6)
PUT key5 = value5 (index 7)
PUT key6 = value6 (index 8)
DEL key4 (index 9)
PUT key7 = value7 (index 10)

--- Generating snapshot at index 10 ---
Snapshot size: 280 bytes
Successfully cleaned up WAL files up to index 10
WAL files after cleanup: 0, Total entries: 0
Remaining WAL files: 
```

## 优势

1. **存储效率**: 基于快照覆盖点智能清理WAL文件，节省存储空间
2. **数据安全**: 确保只有完全被快照覆盖的WAL文件才被删除
3. **性能优化**: 多文件分割减少单个文件大小，提高读写性能
4. **可靠性**: 快照+WAL双重保证，确保数据一致性
5. **可配置性**: 支持自定义每个文件的最大条目数和快照阈值
6. **监控友好**: 提供WAL统计信息，便于监控和调试

## 注意事项

1. **快照阈值对齐**: 建议将快照阈值与MaxWALEntries设置为相同值
2. **文件编号递增**: 删除WAL文件后，新建文件的编号会自动递增
3. **原子操作**: 文件删除操作是原子的，避免数据丢失
4. **错误处理**: 完善的错误处理机制，确保系统稳定性
5. **并发安全**: 使用读写锁保护共享数据结构 