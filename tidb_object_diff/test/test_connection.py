# -*- coding: utf-8 -*-
import pymysql
from test.config import DB_CONFIG
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_connection():
    try:
        # 尝试连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        logging.info("数据库连接成功！")
        
        # 测试基本查询
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logging.info(f"TiDB 版本: {version[0]}")
            
            # 测试序列相关权限
            cursor.execute("SHOW GRANTS")
            grants = cursor.fetchall()
            logging.info("当前用户权限:")
            for grant in grants:
                logging.info(grant[0])
                
        conn.close()
        logging.info("测试完成")
        return True
        
    except Exception as e:
        logging.error(f"连接测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection() 