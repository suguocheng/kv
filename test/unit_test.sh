#!/bin/bash

# KV系统单元测试脚本
# 自动执行所有Go单元测试并生成报告
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
LOGS_DIR="$TEST_DIR/logs"
REPORTS_DIR="$TEST_DIR/reports"

# 确保目录存在
mkdir -p "$RESULTS_DIR" "$LOGS_DIR" "$REPORTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/unit_test_results_$TIMESTAMP.txt"
LOG_FILE="$LOGS_DIR/unit_test_log_$TIMESTAMP.txt"
REPORT_FILE="$REPORTS_DIR/unit_test_report_$TIMESTAMP.txt"
COVERAGE_FILE="$RESULTS_DIR/coverage_$TIMESTAMP.out"
COVERAGE_HTML="$RESULTS_DIR/coverage_$TIMESTAMP.html"

# 测试计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

print_header "KV系统单元测试" "测试时间: $(date) | 项目根目录: $PROJECT_ROOT"

# 切换到项目根目录
cd "$PROJECT_ROOT"

# 清空结果文件
> "$RESULT_FILE"
> "$LOG_FILE"

# 运行测试函数
run_package_test() {
    local package_name="$1"
    local package_path="$2"
    local test_name="$3"
    
    print_subsection "测试包: $package_name"
    echo "路径: $package_path"
    
    # 运行测试并收集覆盖率
    local output
    local exit_code
    
    if [ -d "$package_path" ]; then
        # 运行测试并收集覆盖率
        local test_cmd="go test -v -coverprofile=coverage.tmp -timeout 120s"
        
        output=$(cd "$package_path" && $test_cmd 2>&1)
        exit_code=$?
        
        # 如果测试成功，收集覆盖率数据
        if [ $exit_code -eq 0 ]; then
            if [ -f "$package_path/coverage.tmp" ]; then
                # 检查覆盖率文件格式
                if head -1 "$package_path/coverage.tmp" | grep -q "mode: set"; then
                    # 如果是第一个文件，直接复制；否则合并
                    if [ ! -f "$COVERAGE_FILE" ]; then
                        cp "$package_path/coverage.tmp" "$COVERAGE_FILE"
                    else
                        # 使用gocovmerge工具合并覆盖率文件（如果可用）
                        if command -v gocovmerge >/dev/null 2>&1; then
                            gocovmerge "$COVERAGE_FILE" "$package_path/coverage.tmp" > "$COVERAGE_FILE.tmp" && mv "$COVERAGE_FILE.tmp" "$COVERAGE_FILE"
                        else
                            # 简单合并（可能产生格式问题）
                            cat "$package_path/coverage.tmp" >> "$COVERAGE_FILE"
                        fi
                    fi
                fi
                rm "$package_path/coverage.tmp"
            fi
        fi
    else
        output="Package directory not found: $package_path"
        exit_code=1
    fi
    
    # 记录到日志文件
    echo "=== [$test_name] $package_name ===" >> "$LOG_FILE"
    echo "路径: $package_path" >> "$LOG_FILE"
    echo "输出: $output" >> "$LOG_FILE"
    echo "退出码: $exit_code" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
    
    # 检查结果
    if [ $exit_code -eq 0 ]; then
        print_success "[$test_name] $package_name"
        echo "[$test_name] PASS - $package_name" >> "$RESULT_FILE"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        
        # 提取测试统计信息
        local test_count=$(echo "$output" | grep -E "PASS|FAIL" | wc -l)
        if [ "$test_count" -gt 0 ]; then
            echo "  测试数量: $test_count"
        fi
        
        # 提取覆盖率信息
        local coverage=$(echo "$output" | grep -o "coverage: [0-9.]*%" | head -1)
        if [ -n "$coverage" ]; then
            echo "  覆盖率: $coverage"
        fi
    else
        print_failure "[$test_name] $package_name"
        echo "[$test_name] FAIL - $package_name" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        
        # 显示错误信息
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

# 生成覆盖率报告
print_section "生成覆盖率报告"

# 使用Go的内置工具生成整个项目的覆盖率
echo -e "${BLUE}正在生成项目覆盖率报告...${NC}"

# 尝试生成整个项目的覆盖率
if cd "$PROJECT_ROOT" && go test -coverprofile="$COVERAGE_FILE" -coverpkg=./pkg/... ./pkg/... 2>/dev/null; then
    # 生成HTML覆盖率报告
    if go tool cover -html="$COVERAGE_FILE" -o "$COVERAGE_HTML" 2>/dev/null; then
        echo -e "${GREEN}✓ 覆盖率报告已生成: $COVERAGE_HTML${NC}"
        
        # 显示总体覆盖率
        total_coverage=$(go tool cover -func="$COVERAGE_FILE" | tail -1 | awk '{print $3}' 2>/dev/null || echo "N/A")
        echo -e "${BLUE}总体覆盖率: $total_coverage${NC}"
    else
        echo -e "${YELLOW}警告: HTML覆盖率报告生成失败，但覆盖率数据已收集${NC}"
    fi
else
    echo -e "${YELLOW}警告: 项目覆盖率生成失败，但不影响测试结果${NC}"
    echo -e "${YELLOW}各包的覆盖率信息已在测试过程中显示${NC}"
fi

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

# 生成简洁的报告文件
cat > "$REPORT_FILE" << EOF
# KV系统单元测试报告

## 测试概览
- **测试时间**: $(date)
- **测试脚本**: unit_test.sh
- **总测试包**: $TOTAL_TESTS
- **通过测试**: $PASSED_TESTS
- **失败测试**: $FAILED_TESTS
- **成功率**: $(if [ "$TOTAL_TESTS" -gt 0 ]; then echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc; else echo "0"; fi)%

## 测试覆盖范围
- 配置管理 (config)
- Watch功能 (watch)
- SkipList数据结构 (skiplist)
- KV存储引擎 (kvstore)
- WAL日志管理 (wal)
- Raft共识算法 (raft)
- 客户端库 (client)
- 服务器实现 (server)

## 覆盖率信息
$(if [ -f "$COVERAGE_FILE" ]; then
    echo "- **总体覆盖率**: $(go tool cover -func="$COVERAGE_FILE" | tail -1 | awk '{print $3}')"
    echo "- **覆盖率报告**: $COVERAGE_HTML"
else
    echo "- **覆盖率**: 未生成"
fi)

## 文件位置
- **详细结果**: $RESULT_FILE (包含每个测试包的详细状态)
- **详细日志**: $LOG_FILE (包含每个测试包的完整输出)
- **测试报告**: $REPORT_FILE (本文件，测试概览)
$(if [ -f "$COVERAGE_HTML" ]; then
    echo "- **覆盖率报告**: $COVERAGE_HTML (HTML格式的覆盖率报告)"
fi)

## 快速查看
EOF

# 只添加失败的测试到报告中
if [ $FAILED_TESTS -gt 0 ]; then
    echo "" >> "$REPORT_FILE"
    echo "## 失败的测试包" >> "$REPORT_FILE"
    echo "以下测试包失败，详细错误信息请查看日志文件：" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    grep "FAIL" "$RESULT_FILE" >> "$REPORT_FILE"
else
    echo "" >> "$REPORT_FILE"
    echo "## 测试状态" >> "$REPORT_FILE"
    echo "✅ 所有测试包通过！" >> "$REPORT_FILE"
fi

echo "" >> "$REPORT_FILE"
echo "---" >> "$REPORT_FILE"
echo "*报告生成时间: $(date)*" >> "$REPORT_FILE"

# 简化的终端输出
echo ""
if [ $FAILED_TESTS -eq 0 ]; then
    print_complete "单元测试完成！所有 $TOTAL_TESTS 个测试包通过"
else
    print_failure "单元测试完成！$FAILED_TESTS/$TOTAL_TESTS 个测试包失败"
fi
echo -e "📊 详细报告: $REPORT_FILE"
echo -e "📋 详细结果: $RESULT_FILE"
echo -e "📝 详细日志: $LOG_FILE"
if [ -f "$COVERAGE_HTML" ]; then
    echo -e "📈 覆盖率报告: $COVERAGE_HTML"
fi

if [ $FAILED_TESTS -eq 0 ]; then
    exit 0
else
    exit 1
fi 