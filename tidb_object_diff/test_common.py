# -*- coding: utf-8 -*-
# coding: utf-8
import unittest, pymysql

from common import kill_sessions, Attr
import logging as log


class TestCommon(unittest.TestCase):

    def conn(self, host, port, user, password):
        """
        创建数据库连接
        :param host:
        :param port:
        :param user:
        :param password:
        :return:
        """
        connection = pymysql.connect(host=host, port=port, user=user, password=password,
                                     database="information_schema")
        return connection

    def test_kill_sessions(self):
        conn = self.conn("192.168.31.201", 4000, 'root', '')
        log.info(f"test_kill_sessions start.")
        # self.assert...
        kill_sessions(conn)
        kill_sessions(conn, Attr(users=["root"]))
        log.info(f"test_kill_sessions end.")


if __name__ == "__main__":
    log.basicConfig(level=log.INFO)
    unittest.main()