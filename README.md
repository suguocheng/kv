## 项目简介

一个基于 Raft 共识协议的分布式 KV 存储。支持基本的 KV 操作、TTL、事务（条件与批量原子）、MVCC 历史版本与压缩、Watch 监听等能力。存储层使用 WAL 与快照保证持久化与快速恢复。

- **语言/运行环境**: Go 1.21+（建议）
- **通信**: gRPC
- **一致性**: Raft（自研实现）
- **数据持久化**: 分段 WAL + Snapshot

---

## 主要特性

- **基本操作**: GET/PUT/DEL、PUT 带 TTL
- **事务**: 条件事务（IF-THEN-ELSE）、无条件批量原子操作
- **MVCC**: 历史版本查询、指定版本读取、范围查询
- **压缩**: 基于修订号的历史压缩（Compact）
- **Watch**: 键变更监听、取消监听、查看活跃监听器
- **可恢复**: WAL 回放 + 周期性快照

---

## 目录结构（摘）

- `cmd/server`：服务器入口（每个节点一个进程）
- `cmd/client`：交互式 CLI 客户端
- `pkg/raft`：Raft 协议实现
- `pkg/kvstore`：KV 存储、MVCC、WAL、快照
- `pkg/proto/kvpb`：gRPC/Protobuf 定义与生成代码
- `pkg/server`：gRPC Server 实现
- `pkg/client`：客户端 SDK
- `scripts`：集群启动/停止/清理脚本
- `test`：测试脚本（单元、集成、E2E、性能）
- `tools`：WAL/快照/raft-state 解析工具

---

## 快速开始

1) 安装依赖

```bash
make deps
```

2) 准备配置

```bash
cp config.example.env config.env
# 如需修改端口/节点数/数据目录等，请编辑 config.env
```

3) 构建可执行文件

```bash
make build
# 生成 bin/server 和 bin/client
```

4) 启动 3 节点集群

```bash
make server
# 等价于 scripts/start_servers.sh，读取 config.env 按节点数启动
```

5) 启动客户端（交互式）

```bash
make client
# 或直接运行 bin/client（若已 build）
```

6) 停止与清理

```bash
make stop       # 停止所有节点
make clean-data # 清理 data/ 下的持久化数据
```

---

## 配置说明（config.env）

- **服务器基础**
  - `SERVER_CLIENT_PORT_BASE`：客户端监听端口起始值（节点 i 为 base+i）
  - `SERVER_PEER_PORT_BASE`：Raft 对等端口起始值（节点 i 为 base+i）
  - `SERVER_NODE_COUNT`：节点数（默认 3）
  - `SERVER_HOST`：监听主机/IP（默认 localhost）
- **WAL/快照**
  - `SERVER_MAX_WAL_ENTRIES`：单个 WAL 文件最大条目数（触发轮转）
  - `SERVER_SNAPSHOT_INTERVAL`：每应用多少条日志做一次快照
- **数据路径**
  - `SERVER_DATA_BASE_PATH`：数据根目录（默认 data）
  - `SERVER_WAL_SUBDIR`：WAL 子目录名（默认 wal）
  - `SERVER_RAFT_STATE_FILE`：Raft 状态文件名（默认 raft-state.pb）
  - `SERVER_SNAPSHOT_FILE`：快照文件名（默认 snapshot.pb）
- **客户端**
  - `CLIENT_SERVERS`：逗号分隔的可连接服务器地址列表（如 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002）
  - `CLIENT_HISTORY_DIR`：CLI 历史命令目录（默认 history）
  - `CLIENT_HISTORY_FILE`：CLI 历史命令文件名（默认 kvcli_history）

说明：服务端从 `config.env` 组合出每个节点的监听地址、数据目录与文件路径（如 `data/node0/wal`、`data/node0/raft-state.pb` 等）。

---

## 服务器与集群

- 每个节点以独立进程启动，对等端口用于 Raft 通信，客户端端口接受外部请求
- 进程入口：`cmd/server/main.go`（内部会按配置启动 Raft、KV、gRPC）
- WAL 与快照：按 `SERVER_SNAPSHOT_INTERVAL` 生成快照，并在安装快照后回放 WAL 恢复最新状态

脚本：
- `scripts/start_servers.sh`：根据 `SERVER_NODE_COUNT` 启动多个节点
- `scripts/stop_servers.sh`：停止所有节点
- `scripts/clean_data.sh`：清理数据目录

---

## 客户端 CLI 使用

在交互式提示符中输入命令（大小写不敏感的命令字）。输入 `HELP` 查看帮助。

CLI 历史会记录在 `history/kvcli_history`。

---

## 构建与运行命令

- 构建：`make build`
- 启动服务器集群：`make server`
- 启动客户端：`make client`
- 依赖：`make deps`
- 停止集群：`make stop`
- 清理数据：`make clean-data`
- 清理构建：`make clean`
- 运行 Go 单测：`make test-go`
- 运行脚本化测试：`make test-unit | test-integration | test-e2e | test-benchmark | test-all`

---

## 测试

测试脚本位于 `test/`：

```bash
cd test
./unit_test.sh
./integration_test.sh
./e2e_test.sh
./benchmark_test.sh
./run_all_tests.sh
```

## 工具（诊断/可视化）

- `tools/readpb.go`：读取 protobuf 文件的通用工具
- `tools/readwal/readsnapshot/readpb`：
  - `tools/readwal/readwal.go`：解析 WAL 文件
  - `tools/readsnapshot/readsnapshot.go`：解析快照

编译/运行这些工具可辅助排查数据恢复、回放与快照内容。

---

## 架构概览

- **Raft**：通过日志复制与选举确保线性一致性
- **存储层**：
  - WAL 顺序记录操作以保证崩溃恢复
  - 周期性快照加速恢复（安装快照后从快照点后的 WAL 继续回放）
  - MVCC 保存键的多版本；支持历史查询与压缩
- **gRPC 接口**：客户端通过 gRPC 访问，服务端同时提供对等节点间的 Raft gRPC

---

## 许可证

本项目用于学习与研究目的，未附带明确许可证。如需在生产环境使用，请先完善测试、监控与安全加固。
