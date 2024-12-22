import sqlite3
import threading
import time

class SQLiteConnectionManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.lock = threading.Lock()
        self.__connect()

    def __connect(self):
        """初始化 SQLite 连接"""
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path, timeout=10)
            self.conn.text_factory = str
    # 获取连接后就上锁，防止多线程操作数据库
    def get_connection(self):
        """获取连接"""
        self.lock.acquire()
        return self.conn

    # 关闭连接时释放锁
    def close(self):
        """关闭连接"""
        if self.conn:
            self.lock.release()
            self.conn.close()
            self.conn = None