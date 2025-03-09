# -*- coding: utf-8 -*-
import unittest
import sys
import os
import re

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test.test_base import TestBase
from test.config import TEST_CONFIG
from main import dump_sequences_ddl, get_sequence_map

class TestSequence(TestBase):
    def test_create_basic_sequence(self):
        """测试创建基本序列"""
        sequence_name = self.create_test_sequence('basic')
        self.assert_sequence_exists(sequence_name)
        
    def test_sequence_nextval(self):
        """测试序列值递增"""
        sequence_name = self.create_test_sequence('nextval')
        
        # 测试连续获取值
        self.cursor.execute(f"SELECT NEXTVAL({sequence_name}) as next_val")
        val1 = self.cursor.fetchone()['next_val']
        
        self.cursor.execute(f"SELECT NEXTVAL({sequence_name}) as next_val")
        val2 = self.cursor.fetchone()['next_val']
        
        self.assertEqual(val2, val1 + 1, "Sequence value did not increment correctly")
        
    def test_sequence_cycle(self):
        """测试序列循环"""
        sequence_name = self.create_test_sequence('cycle',
            start=1,
            increment=1,
            minvalue=1,
            maxvalue=3,
            cycle=True
        )
        
        # 获取到最大值
        values = []
        for _ in range(4):
            self.cursor.execute(f"SELECT NEXTVAL({sequence_name}) as next_val")
            values.append(self.cursor.fetchone()['next_val'])
            
        self.assertEqual(values, [1, 2, 3, 1], "Sequence did not cycle correctly")
        
    def test_sequence_cache(self):
        """测试序列缓存"""
        sequence_name = self.create_test_sequence('cache',
            start=1,
            cache=10
        )
        
        # 连续获取多个值
        values = []
        for _ in range(5):
            self.cursor.execute(f"SELECT NEXTVAL({sequence_name}) as next_val")
            values.append(self.cursor.fetchone()['next_val'])
            
        self.assertEqual(values, [1, 2, 3, 4, 5], "Sequence cache not working correctly")
        
    def test_dump_sequences_ddl(self):
        """测试导出序列DDL"""
        test_cases = [
            {
                'suffix': 'basic',
                'params': {
                    'start': 1,
                    'increment': 1,
                    'minvalue': 1,
                    'maxvalue': 1000000,
                    'cache': 1000,
                    'cycle': False
                }
            },
            {
                'suffix': 'cycle',
                'params': {
                    'start': 1,
                    'increment': 2,
                    'minvalue': -100,
                    'maxvalue': 100,
                    'cache': 10,
                    'cycle': True
                }
            },
            {
                'suffix': 'nocache',
                'params': {
                    'start': 100,
                    'increment': 10,
                    'minvalue': 0,
                    'maxvalue': 1000,
                    'cache': 0,
                    'cycle': False
                }
            }
        ]
        
        created_sequences = []
        try:
            # 创建测试序列
            for test_case in test_cases:
                sequence_name = self.create_test_sequence(test_case['suffix'], **test_case['params'])
                created_sequences.append(sequence_name)
                
                # 获取一些值以确保nextval更新
                for _ in range(3):
                    self.cursor.execute(f"SELECT NEXTVAL({sequence_name})")
            
            # 获取DDL
            ddls = dump_sequences_ddl(self.conn, schema_filter=[TEST_CONFIG['test_schema']])
            
            # 验证每个序列的DDL
            for sequence_name in created_sequences:
                full_sequence_name = f"{TEST_CONFIG['test_schema']}.{sequence_name}"
                self.assertIn(full_sequence_name, ddls, f"Missing DDL for sequence {full_sequence_name}")
                ddl = ddls[full_sequence_name].lower()
                
                # 基本语法检查
                self.assertIn('create sequence', ddl)
                self.assertIn(sequence_name.lower(), ddl)
                
                # 验证DDL包含所有必要参数
                required_params = ['start with', 'minvalue', 'maxvalue', 'increment by', 'cache', 'comment']
                for param in required_params:
                    self.assertIn(param, ddl, f"Missing parameter {param} in DDL")
                    
                # 验证参数值格式
                self.assertTrue(re.search(r'start with \d+', ddl), "Invalid START WITH format")
                self.assertTrue(re.search(r'minvalue -?\d+', ddl), "Invalid MINVALUE format")
                self.assertTrue(re.search(r'maxvalue \d+', ddl), "Invalid MAXVALUE format")
                self.assertTrue(re.search(r'increment by \d+', ddl), "Invalid INCREMENT BY format")
                
                # 验证缓存和循环设置
                if 'nocache' in sequence_name:
                    self.assertIn('nocache', ddl)
                else:
                    self.assertTrue(re.search(r'cache \d+', ddl), "Invalid CACHE format")
                    
                if 'cycle' in sequence_name:
                    self.assertIn('cycle', ddl)
                else:
                    self.assertIn('nocycle', ddl)
                    
            # 测试recreate_flag参数
            ddls_no_drop = dump_sequences_ddl(self.conn, schema_filter=[TEST_CONFIG['test_schema']], recreate_flag=False)
            for sequence_name in created_sequences:
                full_sequence_name = f"{TEST_CONFIG['test_schema']}.{sequence_name}"
                ddl = ddls_no_drop[full_sequence_name].lower()
                self.assertNotIn('drop sequence', ddl, "DDL should not contain DROP SEQUENCE when recreate_flag is False")
                
        finally:
            # 清理测试序列
            for sequence_name in created_sequences:
                try:
                    self.cursor.execute(f"DROP SEQUENCE IF EXISTS {sequence_name}")
                except:
                    pass
            self.conn.commit()
        
    def test_get_sequence_map(self):
        """测试获取序列映射"""
        # 创建测试序列
        sequence_name = self.create_test_sequence('map')
        
        # 获取序列映射
        seq_map = get_sequence_map(self.conn, schema_filter=[TEST_CONFIG['test_schema']])
        
        # 验证序列存在于映射中
        full_sequence_name = f"{TEST_CONFIG['test_schema']}.{sequence_name}"
        self.assertIn(full_sequence_name, seq_map)
        
        # 验证序列属性
        seq = seq_map[full_sequence_name]
        self.assertEqual(seq.sequence_schema, TEST_CONFIG['test_schema'])
        self.assertEqual(seq.sequence_name, sequence_name)
        
    def test_sequence_with_special_values(self):
        """测试特殊值的序列"""
        sequence_name = self.create_test_sequence('special',
            start=1000,
            increment=10,
            minvalue=-1000,
            maxvalue=1000000
        )
        
        self.cursor.execute(f"SELECT NEXTVAL({sequence_name}) as next_val")
        val = self.cursor.fetchone()['next_val']
        self.assertEqual(val, 1000)
        
        self.cursor.execute(f"SELECT NEXTVAL({sequence_name}) as next_val")
        val = self.cursor.fetchone()['next_val']
        self.assertEqual(val, 1010)

if __name__ == '__main__':
    unittest.main() 