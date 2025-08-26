#!/bin/bash

# 获取脚本所在目录的上级目录（项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 切换到项目根目录
cd "$PROJECT_ROOT"

NODES=${1:-3}
CONFIG=config.env

if [ ! -f "$CONFIG" ]; then
  echo "配置文件 $CONFIG 不存在，请先手动创建并编辑。"
  exit 1
fi

mkdir -p log bin

# 预编译服务端二进制
if [ ! -x "bin/server" ]; then
  echo "编译服务端二进制: bin/server"
  go build -o bin/server ./cmd/server
fi

echo "启动 $NODES 个KV服务器(bin/server)..."

for ((i=0; i<NODES; i++)); do
  LOGFILE="log/test${i}.log"
  echo "bin/server $i > $LOGFILE 2>&1 &"
  nohup bin/server $i > "$LOGFILE" 2>&1 &
done

echo "所有节点已启动"