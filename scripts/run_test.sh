#!/bin/bash

# 测试运行脚本
# 用于快速测试 Stream Load 导入功能

set -e

echo "=========================================="
echo "Stream Load Doris 测试脚本"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查 MySQL 连接
echo -e "${YELLOW}[1/5] 检查 MySQL 连接...${NC}"
if ! mysql -h localhost -u root -proot -e "SELECT 1" > /dev/null 2>&1; then
    echo -e "${RED}错误: 无法连接到 MySQL${NC}"
    echo "请确保:"
    echo "  1. MySQL 服务正在运行"
    echo "  2. 用户名密码正确 (默认: root/root)"
    echo "  3. 可以修改脚本中的连接参数"
    exit 1
fi
echo -e "${GREEN}✓ MySQL 连接成功${NC}"

# 初始化测试数据
echo -e "${YELLOW}[2/5] 初始化测试数据...${NC}"
mysql -h localhost -u root -proot -e "CREATE DATABASE IF NOT EXISTS test_db"
mysql -h localhost -u root -proot test_db < scripts/init_test_data.sql
echo -e "${GREEN}✓ 测试数据已准备${NC}"

# 检查 Doris 连接
echo -e "${YELLOW}[3/5] 检查 Doris 连接...${NC}"
if ! curl -s http://localhost:8030/api/health > /dev/null 2>&1; then
    echo -e "${RED}警告: 无法连接到 Doris (http://localhost:8030)${NC}"
    echo "请确保:"
    echo "  1. Doris FE 服务正在运行"
    echo "  2. HTTP 端口 8030 可访问"
    echo "  3. 可以修改 application-test.yml 中的配置"
    echo ""
    echo "是否继续? (y/n)"
    read -r response
    if [[ "$response" != "y" ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}✓ Doris 连接成功${NC}"
fi

# 创建 Doris 目标表
echo -e "${YELLOW}[4/5] 创建 Doris 目标表...${NC}"
if command -v mysql &> /dev/null; then
    mysql -h localhost -P 9030 -u root < scripts/init_doris_table.sql 2>/dev/null || {
        echo -e "${YELLOW}提示: 请手动执行 scripts/init_doris_table.sql 在 Doris 中创建目标表${NC}"
    }
fi

# 运行导入程序
echo -e "${YELLOW}[5/5] 启动导入程序...${NC}"
echo ""
echo "使用配置: application-test.yml"
echo "日志级别: DEBUG"
echo ""

# 使用 test profile 运行
mvn spring-boot:run -Dspring-boot.run.profiles=test

echo ""
echo -e "${GREEN}=========================================="
echo "测试完成!"
echo -e "==========================================${NC}"
