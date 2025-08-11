#!/bin/bash

# KV系统完整测试套件
# 运行所有类型的测试：单元测试、功能测试、集成测试、性能基准测试

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 测试脚本路径
UNIT_TEST_SCRIPT="$SCRIPT_DIR/unit_test.sh"
E2E_TEST_SCRIPT="$SCRIPT_DIR/e2e_test.sh"
INTEGRATION_TEST_SCRIPT="$SCRIPT_DIR/integration_test.sh"
CLUSTER_TEST_SCRIPT="$SCRIPT_DIR/cluster_test.sh"
BENCHMARK_TEST_SCRIPT="$SCRIPT_DIR/benchmark_test.sh"

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"
LOGS_DIR="$TEST_DIR/logs"
REPORTS_DIR="$TEST_DIR/reports"
FINAL_REPORT_DIR="$TEST_DIR/final_reports"

# 确保目录存在
mkdir -p "$RESULTS_DIR" "$LOGS_DIR" "$REPORTS_DIR" "$FINAL_REPORT_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
FINAL_REPORT_FILE="$FINAL_REPORT_DIR/comprehensive_test_report_$TIMESTAMP.md"
SUMMARY_FILE="$FINAL_REPORT_DIR/test_summary_$TIMESTAMP.txt"

# 测试结果统计
TOTAL_TEST_SUITES=0
PASSED_TEST_SUITES=0
FAILED_TEST_SUITES=0

# 更新测试套件总数
EXPECTED_TEST_SUITES=5

print_header "KV系统完整测试套件" "测试时间: $(date) | 项目路径: $PROJECT_ROOT"

# 检查测试脚本是否存在
check_test_script() {
    local script_path="$1"
    local script_name="$2"
    
    if [ ! -f "$script_path" ]; then
        print_failure "测试脚本不存在: $script_name"
        return 1
    fi
    
    if [ ! -x "$script_path" ]; then
        print_warning "设置执行权限: $script_name"
        chmod +x "$script_path"
    fi
    
    return 0
}

# 运行测试套件
run_test_suite() {
    local suite_name="$1"
    local script_path="$2"
    local script_name="$3"
    
    TOTAL_TEST_SUITES=$((TOTAL_TEST_SUITES + 1))
    
    print_section "开始运行: $suite_name"
    
    # 检查脚本是否存在
    if ! check_test_script "$script_path" "$script_name"; then
        print_failure "[$TOTAL_TEST_SUITES] $suite_name (脚本不存在)"
        FAILED_TEST_SUITES=$((FAILED_TEST_SUITES + 1))
        return 1
    fi
    
    # 运行测试
    local start_time=$(date +%s)
    if bash "$script_path"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_success "[$TOTAL_TEST_SUITES] $suite_name (耗时: ${duration}秒)"
        PASSED_TEST_SUITES=$((PASSED_TEST_SUITES + 1))
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_failure "[$TOTAL_TEST_SUITES] $suite_name (耗时: ${duration}秒)"
        FAILED_TEST_SUITES=$((FAILED_TEST_SUITES + 1))
        return 1
    fi
}

# 检查所有测试脚本
echo -e "${BLUE}检查测试脚本...${NC}"
check_test_script "$UNIT_TEST_SCRIPT" "unit_test.sh"
check_test_script "$E2E_TEST_SCRIPT" "e2e_test.sh"
check_test_script "$INTEGRATION_TEST_SCRIPT" "integration_test.sh"
check_test_script "$CLUSTER_TEST_SCRIPT" "cluster_test.sh"
check_test_script "$BENCHMARK_TEST_SCRIPT" "benchmark_test.sh"
echo ""

# 运行测试套件 - 按测试金字塔顺序执行
echo -e "${BLUE}开始运行测试套件 (按测试金字塔顺序)...${NC}"
echo ""

# 1. 单元测试 (最底层，最快，最基础)
print_info "第1阶段: 单元测试 - 测试单个组件功能"
run_test_suite "单元测试" "$UNIT_TEST_SCRIPT" "unit_test.sh"
if [ $FAILED_TEST_SUITES -gt 0 ]; then
    print_failure "单元测试失败，停止后续测试"
    echo "单元测试是基础，失败时上层测试很可能也会失败"
    echo "请修复单元测试问题后重新运行"
    exit 1
fi
echo ""

# 2. 集成测试 (组件间交互)
print_info "第2阶段: 集成测试 - 测试组件间交互"
run_test_suite "集成测试" "$INTEGRATION_TEST_SCRIPT" "integration_test.sh"
if [ $FAILED_TEST_SUITES -gt 1 ]; then
    print_failure "集成测试失败，停止后续测试"
    echo "集成测试失败表明组件间交互有问题"
    echo "请修复集成问题后重新运行"
    exit 1
fi
echo ""

# 3. 集群测试 (分布式功能)
print_info "第3阶段: 集群测试 - 测试分布式共识"
run_test_suite "集群测试" "$CLUSTER_TEST_SCRIPT" "cluster_test.sh"
if [ $FAILED_TEST_SUITES -gt 2 ]; then
    print_failure "集群测试失败，停止后续测试"
    echo "集群测试失败表明分布式功能有问题"
    echo "请修复集群问题后重新运行"
    exit 1
fi
echo ""

# 4. 端到端测试 (完整用户场景)
print_info "第4阶段: 端到端测试 - 测试完整用户场景"
run_test_suite "端到端测试" "$E2E_TEST_SCRIPT" "e2e_test.sh"
if [ $FAILED_TEST_SUITES -gt 3 ]; then
    print_failure "端到端测试失败"
    echo "端到端测试失败表明完整流程有问题"
    echo "但基础功能已通过，问题可能在集成层面"
fi
echo ""

# 5. 性能基准测试 (最后执行，最耗时)
print_info "第5阶段: 性能基准测试 - 测试系统性能"
run_test_suite "性能基准测试" "$BENCHMARK_TEST_SCRIPT" "benchmark_test.sh"
echo ""

# 生成综合报告
echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}生成综合测试报告...${NC}"
echo -e "${CYAN}=====================================${NC}"

# 查找最新的测试报告
find_latest_report() {
    local pattern="$1"
    find "$REPORTS_DIR" -name "$pattern" -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-
}

UNIT_REPORT=$(find_latest_report "*unit_test_report*")
E2E_REPORT=$(find_latest_report "*e2e_test_report*")
INTEGRATION_REPORT=$(find_latest_report "*integration_test_report*")
CLUSTER_REPORT=$(find_latest_report "*cluster_test_report*")
BENCHMARK_REPORT=$(find_latest_report "*benchmark_test_report*")

# 生成综合报告
cat > "$FINAL_REPORT_FILE" << EOF
# KV系统综合测试报告

## 测试概览
- **测试时间**: $(date)
- **测试套件**: 完整测试套件 (run_all_tests.sh)
- **测试套件总数**: $EXPECTED_TEST_SUITES
- **通过套件**: $PASSED_TEST_SUITES
- **失败套件**: $FAILED_TEST_SUITES
- **成功率**: $(echo "scale=1; $PASSED_TEST_SUITES * 100 / $EXPECTED_TEST_SUITES" | bc)%

## 测试套件状态 (按执行顺序)

### 1. 单元测试 (第1阶段)
- **状态**: $([ -f "$UNIT_REPORT" ] && echo "✅ 已完成" || echo "❌ 未完成")
- **报告**: $([ -f "$UNIT_REPORT" ] && echo "$UNIT_REPORT" || echo "无")
- **说明**: 测试单个组件功能，最基础最快速

### 2. 集成测试 (第2阶段)
- **状态**: $([ -f "$INTEGRATION_REPORT" ] && echo "✅ 已完成" || echo "❌ 未完成")
- **报告**: $([ -f "$INTEGRATION_REPORT" ] && echo "$INTEGRATION_REPORT" || echo "无")
- **说明**: 测试组件间交互，验证系统集成

### 3. 集群测试 (第3阶段)
- **状态**: $([ -f "$CLUSTER_REPORT" ] && echo "✅ 已完成" || echo "❌ 未完成")
- **报告**: $([ -f "$CLUSTER_REPORT" ] && echo "$CLUSTER_REPORT" || echo "无")
- **说明**: 测试分布式共识算法，验证集群功能

### 4. 端到端测试 (第4阶段)
- **状态**: $([ -f "$E2E_REPORT" ] && echo "✅ 已完成" || echo "❌ 未完成")
- **报告**: $([ -f "$E2E_REPORT" ] && echo "$E2E_REPORT" || echo "无")
- **说明**: 测试完整用户场景，端到端验证

### 5. 性能基准测试 (第5阶段)
- **状态**: $([ -f "$BENCHMARK_REPORT" ] && echo "✅ 已完成" || echo "❌ 未完成")
- **报告**: $([ -f "$BENCHMARK_REPORT" ] && echo "$BENCHMARK_REPORT" || echo "无")
- **说明**: 测试系统性能，最耗时但重要



## 测试覆盖范围 (按测试金字塔顺序)

### 1. 单元测试覆盖 (第1阶段 - 最基础)
- 配置管理
- WAL (Write-Ahead Log) 功能
- 跳表数据结构
- Watch事件系统
- Raft共识算法
- KV存储核心功能
- 事务处理
- MVCC (多版本并发控制)
- TTL (生存时间)
- 客户端功能
- 协议缓冲区

### 2. 集成测试覆盖 (第2阶段 - 组件交互)
- 分布式一致性
- 领导者故障转移
- 网络分区处理
- 数据持久化
- 并发客户端
- 大规模数据处理
- 事务一致性
- MVCC版本控制
- TTL过期机制
- Watch事件系统
- 性能基准测试
- 错误处理
- 快照和压缩
- 负载均衡

### 3. 集群测试覆盖 (第3阶段 - 分布式功能)
- Raft分布式一致性算法
- Server gRPC服务
- WAL并发写入
- 集群协调
- 领导者选举
- 日志复制
- 网络分区处理

### 4. 端到端测试覆盖 (第4阶段 - 完整场景)
- 基本操作 (PUT/GET/DEL)
- 分布式一致性
- 数据持久化
- 完整的用户场景
- 从客户端到服务器的端到端验证

### 5. 性能基准测试覆盖 (第5阶段 - 最耗时)
- PUT操作性能
- GET操作性能
- 并发操作性能
- 事务性能
- 范围查询性能
- 大规模数据处理性能
- TTL操作性能
- 内存使用性能
- 网络延迟性能
- 连接池性能 (gRPC连接复用、并发管理、连接池配置)
- 混合负载性能

## 系统信息
- **操作系统**: $(uname -s)
- **架构**: $(uname -m)
- **CPU核心数**: $(nproc)
- **内存**: $(free -h | awk 'NR==2{print $2}')
- **Go版本**: $(go version)

## 文件位置
- **综合报告**: $FINAL_REPORT_FILE
- **测试结果目录**: $RESULTS_DIR
- **测试日志目录**: $LOGS_DIR
- **测试报告目录**: $REPORTS_DIR

## 快速查看
EOF

# 添加失败的测试信息
if [ $FAILED_TEST_SUITES -gt 0 ]; then
    echo "" >> "$FINAL_REPORT_FILE"
    echo "## 失败的测试套件" >> "$FINAL_REPORT_FILE"
    echo "以下测试套件失败，请查看详细报告：" >> "$FINAL_REPORT_FILE"
    echo "" >> "$FINAL_REPORT_FILE"
    
    if [ ! -f "$UNIT_REPORT" ]; then
        echo "- ❌ 单元测试" >> "$FINAL_REPORT_FILE"
    fi
    if [ ! -f "$E2E_REPORT" ]; then
        echo "- ❌ 端到端测试" >> "$FINAL_REPORT_FILE"
    fi
    if [ ! -f "$INTEGRATION_REPORT" ]; then
        echo "- ❌ 集成测试" >> "$FINAL_REPORT_FILE"
    fi
    if [ ! -f "$CLUSTER_REPORT" ]; then
        echo "- ❌ 集群测试" >> "$FINAL_REPORT_FILE"
    fi
    if [ ! -f "$BENCHMARK_REPORT" ]; then
        echo "- ❌ 性能基准测试" >> "$FINAL_REPORT_FILE"
    fi
else
    echo "" >> "$FINAL_REPORT_FILE"
    echo "## 测试状态" >> "$FINAL_REPORT_FILE"
    echo "✅ 所有测试套件通过！" >> "$FINAL_REPORT_FILE"
fi

echo "" >> "$FINAL_REPORT_FILE"
echo "---" >> "$FINAL_REPORT_FILE"
echo "*综合报告生成时间: $(date)*" >> "$FINAL_REPORT_FILE"

# 生成简要总结
cat > "$SUMMARY_FILE" << EOF
KV系统测试总结 - $(date)
=====================================
测试套件总数: $TOTAL_TEST_SUITES
通过套件: $PASSED_TEST_SUITES
失败套件: $FAILED_TEST_SUITES
成功率: $(echo "scale=1; $PASSED_TEST_SUITES * 100 / $EXPECTED_TEST_SUITES" | bc)%

测试状态: $([ $FAILED_TEST_SUITES -eq 0 ] && echo "✅ 全部通过" || echo "❌ 有失败")

详细报告: $FINAL_REPORT_FILE
EOF

# 最终输出
print_header "测试套件执行完成"

if [ $FAILED_TEST_SUITES -eq 0 ]; then
    print_complete "所有测试套件通过！"
    print_success "   - 单元测试 (第1阶段): ✅"
    print_success "   - 集成测试 (第2阶段): ✅"
    print_success "   - 集群测试 (第3阶段): ✅"
    print_success "   - 端到端测试 (第4阶段): ✅"
    print_success "   - 性能基准测试 (第5阶段): ✅"
else
    print_failure "$FAILED_TEST_SUITES/$EXPECTED_TEST_SUITES 个测试套件失败"
    echo ""
    echo -e "${YELLOW}测试执行顺序说明:${NC}"
    echo "  1. 单元测试 - 最基础，最快 (失败时停止)"
    echo "  2. 集成测试 - 组件交互 (失败时停止)"
    echo "  3. 集群测试 - 分布式功能 (失败时停止)"
    echo "  4. 端到端测试 - 完整场景"
    echo "  5. 性能测试 - 最耗时，最后执行"
fi

echo ""
echo -e "${BLUE}📊 综合报告: $FINAL_REPORT_FILE${NC}"
echo -e "${BLUE}📋 测试总结: $SUMMARY_FILE${NC}"
echo -e "${BLUE}📁 详细结果: $RESULTS_DIR${NC}"
echo -e "${BLUE}📝 详细日志: $LOGS_DIR${NC}"
echo -e "${BLUE}📄 详细报告: $REPORTS_DIR${NC}"

echo ""
echo -e "${YELLOW}测试完成时间: $(date)${NC}"

if [ $FAILED_TEST_SUITES -eq 0 ]; then
    exit 0
else
    exit 1
fi 