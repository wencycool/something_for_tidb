# -*- coding: utf-8 -*-
import unittest
import sys
import os
import logging

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_all_tests():
    """运行所有测试用例"""
    # 设置日志级别
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 自动发现并加载所有测试用例
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(
        start_dir=os.path.dirname(os.path.abspath(__file__)),
        pattern='test_*.py'
    )
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 返回测试结果，用于CI/CD集成
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_all_tests()
    # 如果测试失败，使用非零退出码
    sys.exit(0 if success else 1) 