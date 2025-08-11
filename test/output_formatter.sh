#!/bin/bash

# KV系统测试输出格式工具
# 提供统一的测试输出格式和颜色支持

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
GRAY='\033[0;37m'
NC='\033[0m' # No Color

# 状态图标
SUCCESS_ICON="✅"
FAILURE_ICON="❌"
WARNING_ICON="⚠️"
INFO_ICON="ℹ️"
PROGRESS_ICON="🔄"
COMPLETE_ICON="🎉"

# 输出格式函数
print_header() {
    local title="$1"
    local subtitle="$2"
    
    echo -e "${CYAN}=====================================${NC}"
    echo -e "${CYAN}    $title${NC}"
    echo -e "${CYAN}=====================================${NC}"
    if [ -n "$subtitle" ]; then
        echo -e "${GRAY}$subtitle${NC}"
    fi
    echo ""
}

print_section() {
    local title="$1"
    echo -e "${BLUE}--- $title ---${NC}"
}

print_subsection() {
    local title="$1"
    echo -e "${PURPLE}### $title${NC}"
}

print_success() {
    local message="$1"
    echo -e "${GREEN}$SUCCESS_ICON $message${NC}"
}

print_failure() {
    local message="$1"
    echo -e "${RED}$FAILURE_ICON $message${NC}"
}

print_warning() {
    local message="$1"
    echo -e "${YELLOW}$WARNING_ICON $message${NC}"
}

print_info() {
    local message="$1"
    echo -e "${BLUE}$INFO_ICON $message${NC}"
}

print_progress() {
    local message="$1"
    echo -e "${CYAN}$PROGRESS_ICON $message${NC}"
}

print_complete() {
    local message="$1"
    echo -e "${GREEN}$COMPLETE_ICON $message${NC}"
}

# 测试结果格式化
format_test_result() {
    local test_name="$1"
    local status="$2"
    local duration="$3"
    local details="$4"
    
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✓ [$test_name]${NC} (${duration}s)"
    else
        echo -e "${RED}✗ [$test_name]${NC} (${duration}s)"
        if [ -n "$details" ]; then
            echo -e "${RED}   错误: $details${NC}"
        fi
    fi
}

# 性能指标格式化
format_performance_metric() {
    local metric_name="$1"
    local value="$2"
    local unit="$3"
    local threshold="$4"
    
    echo -e "${WHITE}$metric_name:${NC} ${CYAN}$value $unit${NC}"
    if [ -n "$threshold" ]; then
        if (( $(echo "$value >= $threshold" | bc -l) )); then
            echo -e "  ${GREEN}✓ 达到目标${NC}"
        else
            echo -e "  ${YELLOW}⚠ 未达到目标 (目标: $threshold $unit)${NC}"
        fi
    fi
}

# 统计信息格式化
format_statistics() {
    local total="$1"
    local passed="$2"
    local failed="$3"
    local duration="$4"
    
    local success_rate=$(echo "scale=1; $passed * 100 / $total" | bc -l)
    
    echo -e "${WHITE}统计信息:${NC}"
    echo -e "  总测试数: ${CYAN}$total${NC}"
    echo -e "  通过测试: ${GREEN}$passed${NC}"
    echo -e "  失败测试: ${RED}$failed${NC}"
    echo -e "  成功率: ${CYAN}${success_rate}%${NC}"
    echo -e "  总耗时: ${CYAN}${duration}秒${NC}"
}

# 生成标准化的测试报告
generate_test_report() {
    local test_type="$1"
    local timestamp="$2"
    local total_tests="$3"
    local passed_tests="$4"
    local failed_tests="$5"
    local duration="$6"
    local coverage="$7"
    
    local report_file="test/reports/${test_type}_test_report_${timestamp}.md"
    
    cat > "$report_file" << EOF
# KV系统${test_type}测试报告

## 测试概览
- **测试类型**: ${test_type}测试
- **测试时间**: $(date)
- **测试脚本**: ${test_type}_test.sh
- **总测试数**: $total_tests
- **通过测试**: $passed_tests
- **失败测试**: $failed_tests
- **成功率**: $(echo "scale=1; $passed_tests * 100 / $total_tests" | bc -l)%
- **测试耗时**: ${duration}秒

## 测试覆盖范围
$(get_test_coverage_info "$test_type")

## 性能指标
$(get_performance_metrics "$test_type")

## 覆盖率信息
$(if [ -n "$coverage" ]; then echo "- **代码覆盖率**: $coverage"; else echo "- **代码覆盖率**: 未收集"; fi)

## 文件位置
- **详细结果**: test/results/${test_type}_test_results_${timestamp}.txt
- **详细日志**: test/logs/${test_type}_test_log_${timestamp}.txt
- **测试报告**: test/reports/${test_type}_test_report_${timestamp}.md

## 测试状态
$(if [ $failed_tests -eq 0 ]; then echo "✅ 所有测试通过！"; else echo "❌ 有 $failed_tests 个测试失败"; fi)

---
*报告生成时间: $(date)*
EOF

    echo "$report_file"
}

# 获取测试覆盖信息
get_test_coverage_info() {
    local test_type="$1"
    
    case "$test_type" in
        "unit")
            echo "- 配置管理 (config)"
            echo "- Watch功能 (watch)"
            echo "- SkipList数据结构 (skiplist)"
            echo "- KV存储引擎 (kvstore)"
            echo "- WAL日志管理 (wal)"
            echo "- Raft共识算法 (raft)"
            echo "- 客户端库 (client)"
            echo "- 服务器实现 (server)"
            ;;
        "functional")
            echo "- 基本操作 (PUT/GET/DEL)"
            echo "- TTL功能"
            echo "- 事务操作"
            echo "- MVCC功能"
            echo "- Watch功能"
            echo "- 服务器重启测试"
            ;;
        "integration")
            echo "- 基本操作测试"
            echo "- 分布式一致性测试"
            echo "- 删除操作测试"
            echo "- TTL过期测试"
            echo "- 并发客户端测试"
            echo "- 大规模数据测试"
            echo "- 事务一致性测试"
            echo "- MVCC版本控制测试"
            echo "- 范围查询测试"
            echo "- 错误处理测试"
            echo "- 数据持久化测试"
            echo "- 性能基准测试"
            ;;
        "benchmark")
            echo "- PUT操作性能"
            echo "- GET操作性能"
            echo "- 事务性能"
            echo "- 范围查询性能"
            echo "- TTL操作性能"
            ;;
        *)
            echo "- 通用测试覆盖"
            ;;
    esac
}

# 获取性能指标
get_performance_metrics() {
    local test_type="$1"
    
    case "$test_type" in
        "benchmark")
            echo "- **吞吐量**: 操作数/秒 (ops/s)"
            echo "- **延迟**: 平均响应时间 (ms)"
            echo "- **并发能力**: 支持的并发客户端数"
            echo "- **内存使用**: 内存占用情况"
            ;;
        *)
            echo "- **执行时间**: 测试执行总时间"
            echo "- **成功率**: 测试通过率"
            ;;
    esac
}

# 生成综合测试报告
generate_comprehensive_report() {
    local timestamp="$1"
    local test_results="$2"
    
    local report_file="test/final_reports/comprehensive_test_report_${timestamp}.md"
    
    cat > "$report_file" << EOF
# KV系统综合测试报告

## 测试概览
- **测试时间**: $(date)
- **测试套件**: 完整测试套件 (run_all_tests.sh)
- **测试套件总数**: 4
- **通过套件**: $(echo "$test_results" | grep -c "PASS")
- **失败套件**: $(echo "$test_results" | grep -c "FAIL")
- **成功率**: $(echo "scale=1; $(echo "$test_results" | grep -c "PASS") * 100 / 4" | bc -l)%

## 测试套件状态

### 1. 单元测试
- **状态**: $(if echo "$test_results" | grep -q "unit.*PASS"; then echo "✅ 已完成"; else echo "❌ 失败"; fi)
- **报告**: test/reports/unit_test_report_${timestamp}.md

### 2. 功能测试
- **状态**: $(if echo "$test_results" | grep -q "functional.*PASS"; then echo "✅ 已完成"; else echo "❌ 失败"; fi)
- **报告**: test/reports/functional_test_report_${timestamp}.md

### 3. 集成测试
- **状态**: $(if echo "$test_results" | grep -q "integration.*PASS"; then echo "✅ 已完成"; else echo "❌ 失败"; fi)
- **报告**: test/reports/integration_test_report_${timestamp}.md

### 4. 性能基准测试
- **状态**: $(if echo "$test_results" | grep -q "benchmark.*PASS"; then echo "✅ 已完成"; else echo "❌ 失败"; fi)
- **报告**: test/reports/benchmark_test_report_${timestamp}.md

## 系统信息
- **操作系统**: $(uname -s)
- **架构**: $(uname -m)
- **CPU核心数**: $(nproc)
- **内存**: $(free -h | awk '/^Mem:/{print $2}')
- **Go版本**: $(go version | cut -d' ' -f3)

## 文件位置
- **综合报告**: test/final_reports/comprehensive_test_report_${timestamp}.md
- **测试结果目录**: test/results
- **测试日志目录**: test/logs
- **测试报告目录**: test/reports

## 测试状态
$(if [ $(echo "$test_results" | grep -c "FAIL") -eq 0 ]; then echo "✅ 所有测试套件通过！"; else echo "❌ 有测试套件失败"; fi)

---
*综合报告生成时间: $(date)*
EOF

    echo "$report_file"
}

# 导出函数供其他脚本使用
export -f print_header print_section print_subsection
export -f print_success print_failure print_warning print_info print_progress print_complete
export -f format_test_result format_performance_metric format_statistics
export -f generate_test_report generate_comprehensive_report 