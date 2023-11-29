# -*- coding: utf-8 -*-
# coding: utf-8

import pymysql
from typing import Dict, List, Tuple, Type
import logging, hashlib
import getpass, argparse

logging.basicConfig(
    level=logging.DEBUG,  # 设置日志级别为 DEBUG，可以根据需要调整
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class Index:
    def __init__(self, table_schema, table_name, key_name, cols):
        self.table_schema = table_schema
        self.table_name = table_name
        self.key_name = key_name
        self.cols = cols


def get_index_map(conn: pymysql.connect, schema_filter: List[str] = []) -> Dict[str, Index]:
    index_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    where_schema_filter = "where table_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    cursor.execute("set session group_concat_max_len = 1024000;")
    cursor.execute(
        f"select table_schema,table_name,key_name,group_concat"
        f"(column_name order by seq_in_index separator ',') as cols from "
        f"information_schema.tidb_indexes {where_schema_filter} group by table_schema,table_name,key_name "
        f"order by table_schema,table_name,key_name ;")
    for row in cursor.fetchall():
        index_map[row[0] + "." + row[1] + "." + row[2]] = Index(table_schema=row[0], table_name=row[1], key_name=row[2],
                                                                cols=row[3])
    cursor.close()
    return index_map


class SimplTable:
    def __init__(self, table_schema, table_name, table_type, auto_increment, tidb_pk_type):
        self.table_schema = table_schema
        self.table_name = table_name
        self.table_type = table_type
        self.auto_increment = auto_increment
        self.tidb_pk_type = tidb_pk_type


def get_simpltable_map(conn: pymysql.connect, schema_filter: List[str] = []) -> Dict[str, SimplTable]:
    simpl_table_map = {}
    cursor = conn.cursor()
    where_schema_filter = "where table_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    cursor.execute(
        f"select table_schema,table_name,table_type,auto_increment,tidb_pk_type from information_schema.tables {where_schema_filter};")
    for row in cursor.fetchall():
        simpl_table_map[row["table_schema"] + "." + row["table_name"]] = SimplTable(table_schema=row["table_schema"],
                                                                                    table_name=row["table_name"],
                                                                                    table_type=row["table_type"],
                                                                                    auto_increment=row[
                                                                                        "auto_increment"],
                                                                                    tidb_pk_type=row["tidb_pk_type"])
    cursor.close()
    return simpl_table_map


class User:
    def __init__(self, user, host, authentication_string, priv_md5=""):
        self.user = user
        self.host = host
        self.authentication_string = authentication_string
        self.priv_md5 = priv_md5


def get_user_map(conn: pymysql.connect) -> Dict[str, User]:
    user_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    cursor.execute(f"select user,host,authentication_string from mysql.user;")
    for row in cursor.fetchall():
        user_map["'" + row[0] + "'@'" + row[1] + "'"] = User(user=row[0], host=row[1], authentication_string=row[2])
    # 获取show grants的md5
    for k in user_map:
        grants = []
        cursor.execute(f"show grants for {k};")
        for row in cursor.fetchall():
            grants.append(row[0])
        list.sort(grants)
        user_map[k].priv_md5 = hashlib.md5(";".join(grants).encode()).hexdigest()
    cursor.close()
    return user_map


# 检查sequence
class Sequence:
    def __init__(self, sequence_schema, sequence_name, cycle, increment, max_value, min_value):
        self.sequence_schema = sequence_schema
        self.sequence_name = sequence_name
        self.cycle = cycle
        self.increment = increment
        self.max_value = max_value
        self.min_value = min_value


def get_sequence_map(conn: pymysql.connect, schema_filter: List[str] = []) -> Dict[str, Sequence]:
    seq_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    where_schema_filter = "where sequence_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    cursor.execute(
        f"select sequence_schema,sequence_name,cycle,increment,max_value,min_value from information_schema.sequences {where_schema_filter};")
    for row in cursor.fetchall():
        seq_map[row[0] + "." + row[1]] = Sequence(sequence_schema=row[0], sequence_name=row[1], cycle=row[2],
                                                  increment=row[3], max_value=row[4], min_value=row[5])
    cursor.close()
    return seq_map


# 检查约束
class Constraints:
    def __init__(self, table_schema, table_name, constraint_name, constraint_type):
        self.table_schema = table_schema
        self.table_name = table_name
        self.constraint_name = constraint_name
        self.constraint_type = constraint_type


def get_constraints_map(conn: pymysql.connect, schema_filter: List[str] = []) -> Dict[str, Constraints]:
    constraints_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    where_schema_filter = "where table_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    cursor.execute(
        f"select table_schema,table_name,constraint_name,constraint_type from table_constraints {where_schema_filter};")
    for row in cursor.fetchall():
        constraints_map[row[0] + "." + row[1] + "." + row[2]] = Constraints(table_schema=row[0], table_name=row[1],
                                                                            constraint_name=row[2],
                                                                            constraint_type=row[3])
    cursor.close()
    return constraints_map


# 检查参数
class Variable:
    # 需要对比的参数列表
    variable_filters = ['tidb_mem_oom_action', 'tidb_mem_quota_analyze',
                        'tidb_mem_quota_query', 'tidb_enable_tmp_storage_on_oom', 'tidb_distsql_scan_concurrency',
                        'tidb_analyze_version', 'tidb_cost_model_version', 'tidb_enable_rate_limit_action',
                        'tidb_txn_mode', 'wait_timeout', 'wait_timeout', 'interactive_timeout',
                        'tidb_enable_auto_analyze',
                        'tidb_auto_analyze_ratio', 'tidb_auto_analyze_start_time', 'tidb_auto_analyze_end_time',
                        'tidb_analyze_skip_column_types', 'time_zone', 'sql_mode']
    config_filters = ['new_collations_enabled_on_first_bootstrap']

    def __init__(self, var_type, var_name, var_value):
        self.var_type = var_type
        self.var_name = var_name
        self.var_value = var_value


def get_variable_map(conn: pymysql.connect) -> Dict[str, Variable]:
    var_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    # 获取系统变量参数
    cursor.execute(f"show variables;")
    for row in cursor.fetchall():
        if row[0] in Variable.variable_filters:
            var_map["variable." + row[0]] = Variable(var_type="variable", var_name=row[0], var_value=row[1])
    cursor.execute(f"show config;")
    for row in cursor.fetchall():
        if row[2] in Variable.config_filters:
            var_map["config." + row[2]] = Variable(var_type="config", var_name=row[0] + "." + row[2], var_value=row[3])
    cursor.close()
    return var_map


# 判断两个对象中的变量值是否完全一致
def compare_objects(obj1, obj2) -> bool:
    # 获取对象的所有属性
    attributes1 = vars(obj1)
    attributes2 = vars(obj2)

    # 比较属性值
    for key, value1 in attributes1.items():
        value2 = attributes2.get(key)
        if value1 != value2:
            return False
    return True


# 比较字典是否存在差异
def get_map_diff(a: Dict[str, Type], b: Dict[str, Type]) -> Dict[str, Tuple]:
    a_plus = [k for k in a if k not in b]
    b_plus = [k for k in b if k not in a]
    a_b_eq = [k for k in a if k in b]
    output_dict = {}
    for k in a_plus:
        output_dict[k] = ("+", "-", "-")
    for k in b_plus:
        output_dict[k] = ("-", "+", "-")
    for k in a_b_eq:
        if not compare_objects(a[k], b[k]):
            output_dict[k] = ("-", "-", "+")
    return output_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="系统表比脚本")
    parser.add_argument('--src-host', help="上游IP地址", required=True)
    parser.add_argument('--tgt-host', help="下游IP地址", required=True)
    parser.add_argument('--tgt-port', help="端口号,默认4000", default=4000, type=int)
    parser.add_argument('--src-port', help="端口号,默认4000", default=4000, type=int)
    parser.add_argument('--user', '-u', help="用户名", default="root")
    parser.add_argument('--password', '-p', help="密码", nargs='?')
    parser.add_argument('--schema-list','-s',help="schema列表，指定多个用分隔符隔开，比如：db1,db2,db3，默认包含所有schema",default="*")
    args = parser.parse_args()
    if args.password is None:
        password = getpass.getpass("Enter your password:")
    schema_filter = args.schema_list.split(',')
    if len(schema_filter) == 0 or schema_filter[0] == "*":
        schema_filter = []
    logging.info(f"schema列表为:{schema_filter}")
    try:
        src_connection = pymysql.connect(host=args.src_host, port=args.src_port, user=args.user, password=args.password,
                                         database="information_schema", cursorclass=pymysql.cursors.DictCursor)
        tgt_connection = pymysql.connect(host=args.tgt_host, port=args.tgt_port, user=args.user, password=args.password,
                                         database="information_schema", cursorclass=pymysql.cursors.DictCursor)
    except pymysql.Error as e:
        print(f"Connect Error:{e}")
    src_table_map = get_simpltable_map(src_connection, schema_filter)
    tgt_table_map = get_simpltable_map(tgt_connection, schema_filter)
    logging.info("检查表情况，")
    # 定义占位符距离
    p1, p2, p2, p3, p4 = 100, 10, 10, 10, 10
    # 表检查标题
    k = "[TABLE]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_table_map, tgt_table_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    # 检查索引
    logging.info("查看索引差异")
    src_index_map = get_index_map(src_connection, schema_filter)
    tgt_index_map = get_index_map(tgt_connection, schema_filter)
    k = "[INDEX]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_index_map, tgt_index_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    # 检查sequence
    logging.info("查看Sequence差异")
    src_sequence_map = get_sequence_map(src_connection, schema_filter)
    tgt_sequence_map = get_sequence_map(tgt_connection, schema_filter)
    k = "[SEQUENCE]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_sequence_map, tgt_sequence_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    src_user_map = get_user_map(src_connection)
    tgt_user_map = get_user_map(tgt_connection)

    # 检查约束
    logging.info("查看约束差异")
    src_constraints_map = get_constraints_map(src_connection, schema_filter)
    tgt_constraints_map = get_constraints_map(tgt_connection, schema_filter)
    k = "[CONSTRAINTS]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_constraints_map, tgt_constraints_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    logging.info("查看用户差异")
    src_user_map = get_user_map(src_connection)
    tgt_user_map = get_user_map(tgt_connection)
    k = "[USER]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_user_map, tgt_user_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    logging.info("查看重点参数差异")
    src_var_map = get_variable_map(src_connection)
    tgt_var_map = get_variable_map(tgt_connection)
    k = "[Variable]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_var_map, tgt_var_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")