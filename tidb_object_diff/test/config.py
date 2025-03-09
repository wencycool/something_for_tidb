# -*- coding: utf-8 -*-
# Database configuration
DB_CONFIG = {
    'host': '192.168.31.201',
    'port': 4000,
    'user': 'root',
    'password': '123',
    'database': 'test',
    'charset': 'utf8mb4'
}

# Test configuration
TEST_CONFIG = {
    'sequence_prefix': 'test_seq_',  # 测试序列名称前缀
    'test_schema': 'test',          # 测试数据库名
    'cleanup_after_test': True      # 测试后是否清理测试数据
} 