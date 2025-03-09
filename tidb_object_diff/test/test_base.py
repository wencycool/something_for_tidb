# -*- coding: utf-8 -*-
import unittest
import pymysql
import logging
import sys
import os
import time

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test.config import DB_CONFIG, TEST_CONFIG

class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """在所有测试开始前执行一次"""
        cls.conn = None
        cls.connect_to_db()
        
    @classmethod
    def tearDownClass(cls):
        """在所有测试结束后执行一次"""
        if cls.conn:
            cls.conn.close()
            
    def setUp(self):
        """每个测试用例开始前执行"""
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        
    def tearDown(self):
        """每个测试用例结束后执行"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if TEST_CONFIG['cleanup_after_test']:
            self.cleanup_test_objects()
            
    @classmethod
    def connect_to_db(cls):
        """连接数据库"""
        try:
            cls.conn = pymysql.connect(**DB_CONFIG)
            logging.info(f"Successfully connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        except Exception as e:
            logging.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def cleanup_test_objects(self):
        """清理测试对象"""
        try:
            # 清理测试序列
            self.cursor.execute(
                f"SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME FROM information_schema.sequences "
                f"WHERE SEQUENCE_NAME LIKE '{TEST_CONFIG['sequence_prefix']}%'"
            )
            for row in self.cursor.fetchall():
                self.cursor.execute(f"DROP SEQUENCE IF EXISTS `{row['SEQUENCE_SCHEMA']}`.`{row['SEQUENCE_NAME']}`")
                
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error cleaning up test objects: {str(e)}")
            self.conn.rollback()
            
    def create_test_sequence(self, name_suffix='', **kwargs):
        """创建测试序列"""
        # 添加时间戳确保序列名称唯一
        timestamp = int(time.time() * 1000)
        sequence_name = f"{TEST_CONFIG['sequence_prefix']}{name_suffix}_{timestamp}"
        
        # 默认序列参数
        default_params = {
            'start': 1,
            'increment': 1,
            'minvalue': 1,
            'maxvalue': 1000000,
            'cache': 1000,
            'cycle': False
        }
        
        # 更新默认参数
        params = {**default_params, **kwargs}
        
        # 构建CREATE SEQUENCE语句
        sql = f"""CREATE SEQUENCE {sequence_name}
                 START WITH {params['start']}
                 INCREMENT BY {params['increment']}
                 MINVALUE {params['minvalue']}
                 MAXVALUE {params['maxvalue']}
                 {'CACHE ' + str(params['cache']) if params['cache'] > 0 else 'NOCACHE'}
                 {'CYCLE' if params['cycle'] else 'NOCYCLE'}"""
                 
        try:
            self.cursor.execute(f"DROP SEQUENCE IF EXISTS {sequence_name}")
            self.cursor.execute(sql)
            self.conn.commit()
            return sequence_name
        except Exception as e:
            self.conn.rollback()
            raise
            
    def assert_sequence_exists(self, sequence_name):
        """断言序列存在"""
        self.cursor.execute(
            "SELECT COUNT(*) as cnt FROM information_schema.sequences "
            f"WHERE SEQUENCE_NAME = '{sequence_name}'"
        )
        result = self.cursor.fetchone()
        self.assertGreater(result['cnt'], 0, f"Sequence {sequence_name} does not exist") 