#!/bin/bash

# KV系统功能测试脚本
# 自动执行完整的KV功能测试用例并验证结果
# 测试覆盖：基本操作、TTL、事务、MVCC、Watch等功能

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_ROOT/scripts/start_servers.sh"
STOP_SCRIPT="$PROJECT_ROOT/scripts/stop_servers.sh"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 客户端命令
CLIENT_CMD="go run ./cmd/client/main.go ./cmd/client/handlers.go ./cmd/client/help.go"

# 测试结果文件
RESULT_FILE="test_results.txt"
LOG_FILE="test_log.txt"

# 清理函数
cleanup() {
    echo -e "\n${YELLOW}清理环境...${NC}"
    if [ -f "$STOP_SCRIPT" ]; then
        echo "停止服务器..."
        bash "$STOP_SCRIPT"
    else
        echo "警告: 停止脚本不存在: $STOP_SCRIPT"
    fi
}

# 设置退出时清理
trap cleanup EXIT

echo -e "${GREEN}开始KV系统功能测试...${NC}"
echo "测试时间: $(date)"
echo "客户端命令: $CLIENT_CMD"
echo "====================================="

# 启动服务器
echo -e "\n${YELLOW}启动KV服务器...${NC}"
if [ -f "$START_SCRIPT" ]; then
    echo "执行启动脚本: $START_SCRIPT"
    bash "$START_SCRIPT"
    
    # 等待服务器启动
    echo "等待服务器启动..."
    sleep 3
    
    # 检查服务器是否启动成功
    echo "检查服务器状态..."
    for port in 8000 8001 8002; do
        if lsof -i :$port > /dev/null 2>&1; then
            echo -e "${GREEN}✓ 端口 $port 服务器已启动${NC}"
        else
            echo -e "${RED}✗ 端口 $port 服务器启动失败${NC}"
            exit 1
        fi
    done
else
    echo -e "${RED}错误: 启动脚本不存在: $START_SCRIPT${NC}"
    exit 1
fi

# 清空结果文件
> "$RESULT_FILE"
> "$LOG_FILE"

# 测试计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# 执行测试函数
run_test() {
    local test_name="$1"
    local command="$2"
    local expected="$3"
    local timeout="${4:-5}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo -e "\n${YELLOW}[$TOTAL_TESTS] $test_name${NC}"
    echo "命令: $command"
    echo "期望: $expected"
    
    # 执行命令
    local output
    output=$(timeout "$timeout"s $CLIENT_CMD <<< "$command" 2>&1)
    local exit_code=$?
    
    # 记录到日志文件
    echo "=== [$TOTAL_TESTS] $test_name ===" >> "$LOG_FILE"
    echo "命令: $command" >> "$LOG_FILE"
    echo "输出: $output" >> "$LOG_FILE"
    echo "退出码: $exit_code" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
    
    # 检查结果
    if [ $exit_code -eq 0 ] && echo "$output" | grep -q "$expected"; then
        echo -e "${GREEN}✓ 通过${NC}"
        echo "输出: $output"
        echo "[$TOTAL_TESTS] PASS - $test_name" >> "$RESULT_FILE"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}✗ 失败${NC}"
        echo "实际输出: $output"
        echo "期望包含: $expected"
        echo "[$TOTAL_TESTS] FAIL - $test_name" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# 等待函数
wait_seconds() {
    local seconds="$1"
    echo -e "\n${YELLOW}等待 $seconds 秒...${NC}"
    sleep "$seconds"
}

# 执行测试用例

# 基本操作测试
run_test "PUT操作" "PUT 1 1" "成功设置键 '1' = '1'"
run_test "GET操作" "GET 1" "1"
run_test "DEL操作" "DEL 1" "成功删除键 '1'"
run_test "GET已删除的键" "GET 1" "键不存在"

# TTL测试
run_test "PUTTTL操作" "PUTTTL 2 2 3" "成功设置键 '2' = '2'"
run_test "GET TTL键" "GET 2" "2"
wait_seconds 3
run_test "TTL过期检查" "GET 2" "键不存在"

# 事务测试
run_test "PUT键3" "PUT 3 3" "成功设置键 '3' = '3'"
run_test "事务-EXISTS条件" "TXN IF EXISTS 3 THEN GET 3 ELSE PUT 3 3" "成功"
run_test "事务-VALUE条件" "TXN IF VALUE 3 = 3 THEN DEL 3 ELSE PUT 3 0" "成功"
run_test "事务-EXISTS条件2" "TXN IF EXISTS 3 THEN GET 3 ELSE PUT 3 3" "成功"
run_test "事务-VALUE条件2" "TXN IF VALUE 3 != 3 THEN GET 3 ELSE PUT 3 0" "成功"
run_test "验证事务结果" "GET 3" "0"

# 批量事务测试
run_test "批量事务" "TXN PUT 4 4 PUT 5 5 PUT 6 6" "成功"
run_test "验证键4" "GET 4" "4"
run_test "验证键5" "GET 5" "5"
run_test "验证键6" "GET 6" "6"

# 版本历史测试
run_test "PUT键1版本2" "PUT 1 2" "成功设置键 '1' = '2'"
run_test "PUT键1版本3" "PUT 1 3" "成功设置键 '1' = '3'"
run_test "PUT键1版本4" "PUT 1 4" "成功设置键 '1' = '4'"
run_test "PUT键1版本5" "PUT 1 5" "成功设置键 '1' = '5'"
run_test "查看版本历史" "HISTORY 1" "共5个版本"
run_test "获取指定版本" "GETREV 1 12" "版本: 12"
run_test "范围查询" "RANGE 1 7" "共6个"

# 压缩测试
run_test "压缩版本" "COMPACT 2" "压缩完成"
run_test "压缩后历史" "HISTORY 1" "共3个版本"

# Watch测试
run_test "PUT键7" "PUT 7 7" "成功设置键 '7' = '7'"
run_test "Watch键7" "WATCH 7" "开始监听"
run_test "修改被监听的键" "PUT 7 8" "成功设置键 '7' = '8'"
# 跳过取消监听测试，因为watcher_id是动态生成的
# run_test "取消监听" "UNWATCH watcher_id" "成功取消监听器"
run_test "删除键7" "DEL 7" "成功删除键 '7'"

# 重启服务器测试
echo -e "\n${YELLOW}[重启服务器测试]${NC}"
echo "重启服务器..."
bash "$STOP_SCRIPT"
wait_seconds 2
bash "$START_SCRIPT"
wait_seconds 3

# 检查服务器是否重启成功
echo "检查服务器重启状态..."
for port in 8000 8001 8002; do
    if lsof -i :$port > /dev/null 2>&1; then
        echo -e "${GREEN}✓ 端口 $port 服务器已重启${NC}"
    else
        echo -e "${RED}✗ 端口 $port 服务器重启失败${NC}"
        exit 1
    fi
done

# 重启后数据验证测试
run_test "重启后HISTORY 1" "HISTORY 1" "共5个版本"
run_test "重启后HISTORY 3" "HISTORY 3" "共1个版本"
run_test "重启后GET 4" "GET 4" "4"
run_test "重启后GET 5" "GET 5" "5"
run_test "重启后GET 6" "GET 6" "6"
run_test "重启后GET 7" "GET 7" "键不存在"

echo -e "\n${GREEN}=====================================${NC}"
echo -e "${GREEN}测试完成！${NC}"
echo "总计: $TOTAL_TESTS"
echo -e "${GREEN}通过: $PASSED_TESTS${NC}"
echo -e "${RED}失败: $FAILED_TESTS${NC}"

# 保存汇总结果
echo "" >> "$RESULT_FILE"
echo "=====================================" >> "$RESULT_FILE"
echo "测试汇总 - $(date)" >> "$RESULT_FILE"
echo "总计: $TOTAL_TESTS" >> "$RESULT_FILE"
echo "通过: $PASSED_TESTS" >> "$RESULT_FILE"
echo "失败: $FAILED_TESTS" >> "$RESULT_FILE"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}所有测试通过！${NC}"
    echo "详细结果请查看: $RESULT_FILE"
    echo "详细日志请查看: $LOG_FILE"
    exit 0
else
    echo -e "${RED}有 $FAILED_TESTS 个测试失败${NC}"
    echo "详细结果请查看: $RESULT_FILE"
    echo "详细日志请查看: $LOG_FILE"
    exit 1
fi 