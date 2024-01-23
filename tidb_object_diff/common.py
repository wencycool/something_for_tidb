# -*- coding: utf-8 -*-
# coding: utf-8
import pymysql
import logging as log
import base64


# 将字符串转为base64编码
def str_to_base64(s):
    return base64.b64encode(s.encode('utf-8')).decode('utf-8')


class Attr:
    """
    kill_sessions的维度信息，可以通过用户、db、实例等维护进行过滤，默认情况下会杀掉所有会话
    """

    def __init__(self, users=[], ids=[], dbs=[], instances=[]):
        """
        :param list[str] users:
        :param list[int] ids:
        :param list[str] dbs:
        :param list[str] instances:
        """
        self.users = users  # 需杀掉的用户列表
        self.ids = ids  # 需杀掉的ID列表
        self.dbs = dbs  # 需杀掉的DB列表
        self.instances = instances  # 需杀掉的instance列表


def kill_sessions(conn: pymysql.connect, attr: Attr = None):
    """
    杀掉数据库连接
    :param conn:连接地址
    :param attr:需要过滤的维度，比如用户名、实例等维度，默认情况下会杀掉所有连接
    :return:
    """
    attr_list = []
    if attr is not None:
        if len(attr.users) != 0:
            user_list = ",".join(list(map(lambda x: f"'{x}'", attr.users)))
            attr_list.append(f" user in ({user_list}) ")
        if len(attr.ids) != 0:
            id_list = ",".join(attr.ids)
            attr_list.append(f" id in ({id_list}) ")
        if len(attr.dbs) != 0:
            db_list = ",".join(list(map(lambda x: f"'{x}'", attr.dbs)))
            attr_list.append(f" db in ({db_list}) ")
        if len(attr.instances) != 0:
            instance_list = ",".join(list(map(lambda x: f"'{x}'", attr.instances)))
            attr_list.append(f" instance in ({instance_list}) ")
    filter_clause = "" if len(attr_list) == 0 else " and " + " and ".join(attr_list)
    show_processlist = f"select instance,id,user,host,db,command,time,state,info from " \
                       f"information_schema.cluster_processlist where id != connection_id() {filter_clause}; "
    log.info(f"show processlist sql:{show_processlist}")

    cursor = conn.cursor(pymysql.cursors.Cursor)
    cursor.execute(show_processlist)
    result = cursor.fetchall()
    log.info(f"total connections need killed:{len(result)}")
    for r in result:
        kill_sql = f"kill tidb {r[1]}"
        log.info(f"kill sql:{kill_sql}")
        cursor.execute(f"kill tidb {r[1]};")
    cursor.close()



