#!/bin/bash

# KV系统单元测试脚本
# 自动执行所有Go单元测试并生成简洁结果
#
# 环境变量:
#   暂无特殊环境变量

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 创建测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"

# 确保目录存在
mkdir -p "$RESULTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/unit_test_results_$TIMESTAMP.txt"

# 测试计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

print_header "KV系统单元测试" "测试时间: $(date) | 项目根目录: $PROJECT_ROOT"

# 切换到项目根目录
cd "$PROJECT_ROOT"

# 清空结果文件
> "$RESULT_FILE"

# 运行测试函数
run_package_test() {
    local package_name="$1"
    local package_path="$2"
    local test_name="$3"
    
    print_subsection "测试包: $package_name"
    echo "路径: $package_path"
    
    local output
    local exit_code
    
    if [ -d "$package_path" ]; then
        local test_cmd="go test -v -timeout 120s"
        output=$(cd "$package_path" && $test_cmd 2>&1)
        exit_code=$?
    else
        output="Package directory not found: $package_path"
        exit_code=1
    fi
    
    if [ $exit_code -eq 0 ]; then
        print_success "[$test_name] $package_name"
        echo "[$test_name] PASS - $package_name" >> "$RESULT_FILE"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        
        local test_count=$(echo "$output" | grep -E "PASS|FAIL" | wc -l)
        if [ "$test_count" -gt 0 ]; then
            echo "  测试数量: $test_count"
        fi
    else
        print_failure "[$test_name] $package_name"
        echo "[$test_name] FAIL - $package_name" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo "$output" | tail -10
    fi
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
}

# 执行各个包的测试
print_section "开始执行单元测试"

# 配置模块测试
run_package_test "config" "pkg/config" "CONFIG"

# Watch模块测试
run_package_test "watch" "pkg/watch" "WATCH"

# SkipList模块测试
run_package_test "skiplist" "pkg/kvstore" "SKIPLIST"

# KV存储模块测试
run_package_test "kvstore" "pkg/kvstore" "KVSTORE"

# WAL模块测试
run_package_test "wal" "pkg/wal" "WAL"

# Raft模块测试
run_package_test "raft" "pkg/raft" "RAFT"

# 客户端模块测试
run_package_test "client" "pkg/client" "CLIENT"

# 服务器模块测试
run_package_test "server" "pkg/server" "SERVER"

# 保存汇总结果到结果文件
echo "" >> "$RESULT_FILE"
echo "=====================================" >> "$RESULT_FILE"
echo "单元测试汇总 - $(date)" >> "$RESULT_FILE"
echo "总计: $TOTAL_TESTS" >> "$RESULT_FILE"
echo "通过: $PASSED_TESTS" >> "$RESULT_FILE"
echo "失败: $FAILED_TESTS" >> "$RESULT_FILE"
if [ "$TOTAL_TESTS" -gt 0 ]; then
    echo "成功率: $(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%" >> "$RESULT_FILE"
fi

# 简化的终端输出
echo ""
if [ $FAILED_TESTS -eq 0 ]; then
    print_complete "单元测试完成！所有 $TOTAL_TESTS 个测试包通过"
else
    print_failure "单元测试完成！$FAILED_TESTS/$TOTAL_TESTS 个测试包失败"
fi
echo -e "📋 详细结果: $RESULT_FILE"

if [ $FAILED_TESTS -eq 0 ]; then
    exit 0
else
    exit 1
fi 