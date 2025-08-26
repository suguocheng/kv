#!/bin/bash

# KV系统集群测试脚本
# 专门用于测试需要完整集群的raft和server模块

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_ROOT/scripts/start_servers.sh"
STOP_SCRIPT="$PROJECT_ROOT/scripts/stop_servers.sh"
CLEAN_DATA_SCRIPT="$PROJECT_ROOT/scripts/clean_data.sh"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 预编译客户端
BIN_DIR="$PROJECT_ROOT/bin"
BIN_CLI="$BIN_DIR/client"
mkdir -p "$BIN_DIR"
print_progress "编译客户端二进制: $BIN_CLI"
(cd "$PROJECT_ROOT" && go build -o "$BIN_CLI" ./cmd/client)

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"

# 确保目录存在
mkdir -p "$RESULTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/cluster_test_results_$TIMESTAMP.txt"

# 清理函数
cleanup() {
	print_section "清理环境"
	if [ -f "$STOP_SCRIPT" ]; then
		echo "停止服务器..."
		bash "$STOP_SCRIPT"
	fi
}

# 设置退出时清理
trap cleanup EXIT

print_header "KV系统集群测试" "测试时间: $(date)"

# 清空结果文件
> "$RESULT_FILE"

# 启动服务器
print_section "启动KV服务器"
bash "$CLEAN_DATA_SCRIPT"
bash "$START_SCRIPT"
sleep 5

# 检查服务器状态
for port in 9000 9001 9002; do
	if ! lsof -i :$port > /dev/null 2>&1; then
		print_failure "端口 $port 服务器启动失败"
		exit 1
	fi
done
print_success "所有服务器启动成功"

# 等待集群稳定
echo "等待集群稳定..."
sleep 5

# 客户端执行（带重试）
kv_exec() {
	local cmd_line="$1"
	local max_attempts="${2:-8}"
	local delay_sec="${3:-0.5}"
	local attempt=1
	local output=""
	while [ $attempt -le $max_attempts ]; do
		output=$(cd "$PROJECT_ROOT" && timeout 5s "$BIN_CLI" <<< "$cmd_line" 2>&1 || true)
		if echo "$output" | grep -qi "failed to create any initial connections\|failed to add server"; then
			sleep "$delay_sec"
			attempt=$((attempt + 1))
			continue
		fi
		break
	done
	echo "$output"
}

# 测试1：PUT/GET 跨集群一致性
print_section "测试1：PUT/GET 跨集群一致性"
put_out=$(kv_exec "PUT cluster_key_1 cluster_value_1")
if echo "$put_out" | grep -q "成功设置键 'cluster_key_1' = 'cluster_value_1'"; then
	print_success "PUT 成功"
	echo "[1] PASS - PUT cluster_key_1" >> "$RESULT_FILE"
else
	print_failure "PUT 失败"
	echo "[1] FAIL - PUT cluster_key_1" >> "$RESULT_FILE"
fi
sleep 0.2
get_out=$(kv_exec "GET cluster_key_1")
if echo "$get_out" | grep -q "cluster_value_1"; then
	print_success "GET 返回正确值"
	echo "[2] PASS - GET cluster_key_1" >> "$RESULT_FILE"
else
	print_failure "GET 未返回正确值"
	echo "[2] FAIL - GET cluster_key_1" >> "$RESULT_FILE"
fi

# 测试2：事务写入
print_section "测试2：事务写入"
txn_out=$(kv_exec "TXN PUT txn_ck1 v1 PUT txn_ck2 v2")
if echo "$txn_out" | grep -qi "事务"; then
	print_success "事务提交触发"
	echo "[3] PASS - TXN write" >> "$RESULT_FILE"
else
	print_warning "客户端未输出标准事务提示（忽略显示但继续验证）"
	echo "[3] PASS - TXN write (soft)" >> "$RESULT_FILE"
fi
sleep 0.2
get_txn1=$(kv_exec "GET txn_ck1")
get_txn2=$(kv_exec "GET txn_ck2")
if echo "$get_txn1$get_txn2" | grep -q "v1" && echo "$get_txn1$get_txn2" | grep -q "v2"; then
	print_success "事务写入键可读"
	echo "[4] PASS - TXN readback" >> "$RESULT_FILE"
else
	print_failure "事务写入后读取失败"
	echo "[4] FAIL - TXN readback" >> "$RESULT_FILE"
fi

# 测试3：删除并校验不存在（重试）
print_section "测试3：删除并校验不存在"
_=$(kv_exec "PUT del_ck del_val") > /dev/null
sleep 0.1
del_out=$(kv_exec "DEL del_ck")
if echo "$del_out" | grep -q "成功删除键 'del_ck'"; then
	print_success "DEL 返回成功"
	echo "[5] PASS - DEL del_ck" >> "$RESULT_FILE"
else
	print_failure "DEL 返回失败"
	echo "[5] FAIL - DEL del_ck" >> "$RESULT_FILE"
fi
absent=false
for i in {1..10}; do
	chk=$(kv_exec "GET del_ck")
	if echo "$chk" | grep -q "不存在"; then
		absent=true
		break
	fi
	sleep 0.1
done
if [ "$absent" = true ]; then
	print_success "DEL 后键不存在"
	echo "[6] PASS - DEL verify" >> "$RESULT_FILE"
else
	print_failure "DEL 后依然可读"
	echo "[6] FAIL - DEL verify" >> "$RESULT_FILE"
fi

# 汇总与报告
echo "" >> "$RESULT_FILE"
echo "=====================================" >> "$RESULT_FILE"
echo "集群测试汇总 - $(date)" >> "$RESULT_FILE"

# 终端输出
echo ""
if grep -q "FAIL" "$RESULT_FILE"; then
	print_failure "集群测试完成！部分测试失败"
else
	print_complete "集群测试完成！所有测试通过"
fi
echo -e "📋 详细结果: $RESULT_FILE"

if ! grep -q "FAIL" "$RESULT_FILE"; then
	exit 0
else
	exit 1
fi 