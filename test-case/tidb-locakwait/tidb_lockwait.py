# coding=utf8
# 模拟mysql高并发存在锁的情况
import pymysql
from dbutils.pooled_db import PooledDB
import concurrent.futures
import time
from random import Random
import logging


class TestTiDBLockWait:
    def __init__(self, pool):
        """
        :type pool: PooledDB
        :param pool: 数据库连接池
        """
        self.pool = pool
        self.ids = None

    # 创建测试表:test
    def create_table(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        cursor.execute("drop table if exists test")
        cursor.execute("create table if not exists test (id int, value varchar(100))")
        conn.commit()
        cursor.close()
        conn.close()

    # 向测试表中插入1000条数据
    def insert_data(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        for i in range(1000):
            cursor.execute("insert into test (id, value) values (%s, %s)", (i, 'a'))
        conn.commit()
        cursor.close()
        conn.close()

    def get_ids(self):
        """
        获取所有id列表
        :rtype: list[int]
        """
        if self.ids:
            return self.ids
        conn = self.pool.connection()
        cursor = conn.cursor()
        # 获取前20条数据
        cursor.execute("select id from test limit 20")
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return [item[0] for item in result]

    # 测试锁等待
    def test_one_lock_wait(self, commit_delay=1):
        """
        测试锁等待
        :type commit_delay: any
        :param commit_delay:延迟commit的时间（秒）
        """
        while True:
            conn = self.pool.connection()
            cursor = conn.cursor()
            try:
                # 随机获取一个id
                id = self.get_ids()[Random().randint(0, len(self.get_ids()) - 1)]
                sql = "update test set value = concat(value, 'a') where id = %s"
                cursor.execute(sql, (id,))
                time.sleep(commit_delay)
                conn.rollback()
            except Exception as e:
                logging.error("Error: %s", e)
            finally:
                cursor.close()
                conn.close()
            logging.debug("update id: %s", id)
            # 休眠一段时间，否则不会触发无限循环
            time.sleep(0.0001)

    def test_lock_wait(self, thread_num=20, commit_delay=1):
        """
        测试锁等待
        :type thread_num: int
        :param thread_num:线程数
        :type commit_delay: any
        :param commit_delay:延迟commit的时间（秒）
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_num) as executor:
            futures = [executor.submit(self.test_one_lock_wait, commit_delay) for i in range(thread_num)]
            for future in concurrent.futures.as_completed(futures):
                future.result()


def main():
    # 打印日志，包括时间，行号，日志级别，日志信息
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    # 数据库连接配置
    db_config = {
        'host': '192.168.31.201',
        'port': 4001,
        'user': 'root',
        'password': '',
        'database': 'test'
    }
    pool = PooledDB(
        creator=pymysql,
        maxconnections=20,
        mincached=10,
        maxcached=10,
        blocking=True,
        **db_config
    )
    logging.info("start test")
    test = TestTiDBLockWait(pool)
    logging.info("create table test")
    test.create_table()
    logging.info("insert data")
    test.insert_data()
    logging.info("test lock wait")
    # 设置线程数为20，延迟commit(rollback)的时间为1秒
    test.test_lock_wait(thread_num=20, commit_delay=1)


if __name__ == "__main__":
    main()
