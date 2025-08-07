#!/bin/bash

# 清理数据目录脚本
# 功能：删除所有节点的WAL文件，并清空raft-state.pb和snapshot.pb文件

set -e  # 遇到错误时退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查data目录是否存在
if [ ! -d "data" ]; then
    print_error "data目录不存在"
    exit 1
fi

print_info "开始清理数据目录..."

# 遍历data目录下的所有node*目录
for node_dir in data/node*; do
    if [ ! -d "$node_dir" ]; then
        continue
    fi
    
    node_name=$(basename "$node_dir")
    print_info "处理节点: $node_name"
    
    # 删除WAL目录下的所有文件
    if [ -d "$node_dir/wal" ]; then
        print_info "  删除WAL文件..."
        rm -rf "$node_dir/wal"/*
        print_info "  WAL文件已删除"
    else
        print_warn "  WAL目录不存在: $node_dir/wal"
    fi
    
    # 清空raft-state.pb文件
    if [ -f "$node_dir/raft-state.pb" ]; then
        print_info "  清空raft-state.pb文件..."
        > "$node_dir/raft-state.pb"
        print_info "  raft-state.pb已清空"
    else
        print_warn "  raft-state.pb文件不存在: $node_dir/raft-state.pb"
    fi
    
    # 清空snapshot.pb文件
    if [ -f "$node_dir/snapshot.pb" ]; then
        print_info "  清空snapshot.pb文件..."
        > "$node_dir/snapshot.pb"
        print_info "  snapshot.pb已清空"
    else
        print_warn "  snapshot.pb文件不存在: $node_dir/snapshot.pb"
    fi
    
    print_info "  节点 $node_name 清理完成"
    echo
done

print_info "所有节点数据清理完成！"
print_warn "注意：这将删除所有WAL日志和清空状态文件，重启服务器后将从头开始" 