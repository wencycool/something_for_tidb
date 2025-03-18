# -*- coding: utf-8 -*-
# coding: utf-8

import pymysql
from typing import Dict, List, Tuple, Type
import logging, hashlib
import getpass, argparse
import re
import sys

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
    def __init__(self, table_schema, table_name, table_type, tidb_pk_type):
        self.table_schema = table_schema
        self.table_name = table_name
        self.table_type = table_type
        self.tidb_pk_type = tidb_pk_type


def get_simpltable_map(conn: pymysql.connect, schema_filter: List[str] = []) -> Dict[str, SimplTable]:
    simpl_table_map = {}
    cursor = conn.cursor()
    where_schema_filter = "where table_type in ('BASE TABLE', 'VIEW') and table_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    cursor.execute(
        f"select table_schema,table_name,table_type,tidb_pk_type from information_schema.tables {where_schema_filter};")
    for row in cursor.fetchall():
        simpl_table_map[row["table_schema"] + "." + row["table_name"]] = SimplTable(table_schema=row["table_schema"],
                                                                                    table_name=row["table_name"],
                                                                                    table_type=row["table_type"],
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


def dump_sequences_ddl(src_conn: pymysql.connect, tgt_conn: pymysql.connect = None, schema_filter: List[str] = [], recreate_flag=True) -> Dict[str, str]:
    """
    导出Sequence创建脚本，按照如下方式执行：
    1. 获取源端和目标端的Sequence信息
    2. 根据原端和目标端的sequence信息来查看目标端sequence是否需要新创建
    3. 如果需要新创建，则根据源端和目标端的sequence信息来生成创建脚本
    4. 对于所有sequence，使用setval方式在当前nextval基础上加上一个步长(一万）作为初始值，导出的Sequence主要用于ticdc的下游使用
    Args:
        src_conn: 源端连接
        tgt_conn: 目标端连接
        schema_filter: 需要导出的schema列表
        recreate_flag: 是否重新创建Sequence
    Returns:
        sequence_map: 导出的Sequence创建脚本
    """
    sequence_map = {}
    target_sequence_map = {} # 目标端sequence信息，对于在目标端已经存在的sequence，无需重复创建，只生成select setval的语句
    src_cursor = src_conn.cursor(pymysql.cursors.DictCursor)
    
    step_plus = 10000  # 需要增加的步长
    where_schema_filter = "where sequence_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    if tgt_conn is not None:
        try:
            tgt_cursor = tgt_conn.cursor(pymysql.cursors.DictCursor)
            tgt_cursor.execute(f"select sequence_schema,sequence_name from information_schema.sequences {where_schema_filter};")
            for row in tgt_cursor.fetchall():
                target_sequence_map[f"`{row['sequence_schema']}`.`{row['sequence_name']}`"] = True
        except Exception as e:
            logging.error(f"Error querying sequences: {str(e)}")
        finally:
            tgt_cursor.close()
        
    try:
        src_cursor.execute(
            f"select sequence_schema,sequence_name,cache,cache_value,cycle,increment,max_value,min_value,start,comment from information_schema.sequences {where_schema_filter};")
        for row in src_cursor.fetchall():
            try:
                src_cursor.execute(f"select nextval(`{row['sequence_schema']}`.`{row['sequence_name']}`) as col")
                result = src_cursor.fetchone()
                current_val = result["col"] if result else 0
                next_val = current_val + step_plus  # 当前基础上加一万作为下一个初始值
                
                # 处理可能为None的数值
                min_value = row["min_value"] if row["min_value"] is not None else 1
                max_value = row["max_value"] if row["max_value"] is not None else (1 << 63) - 1
                increment = row["increment"] if row["increment"] is not None else 1
                cache_value = row["cache_value"] if row["cache_value"] is not None else 1000
                comment = row["comment"] if row["comment"] is not None else ""
                # todo 这里需要判断next_val是否大于max_value，目前没有判断
                # 转义comment中的特殊字符
                comment = comment.replace("'", "\\'")
                sequence_name = f"`{row['sequence_schema']}`.`{row['sequence_name']}`"
                if sequence_name in target_sequence_map:
                    sequence_map[sequence_name] = f"select setval({sequence_name}, {next_val});"
                else:
                    drop_sequence_ddl = f"drop sequence if exists {sequence_name};"
                    create_sequence_ddl = "%screate sequence %s start with %d minvalue %d maxvalue %d increment by %d %s %s comment='%s';" % (
                        drop_sequence_ddl if recreate_flag else "",
                        sequence_name,
                        next_val,
                        min_value,
                        max_value,
                        increment,
                        "nocache" if row.get("cache", 0) == 0 else f"cache {cache_value}",
                        "nocycle" if row.get("cycle", 0) == 0 else "cycle",
                        comment
                    )
                    sequence_map[sequence_name] = create_sequence_ddl
            except Exception as e:
                logging.error(f"Error processing sequence {row['sequence_schema']}.{row['sequence_name']}: {str(e)}")
                continue
    except Exception as e:
        logging.error(f"Error querying sequences: {str(e)}")
    finally:
        src_cursor.close()
    return sequence_map


def get_sequence_map(conn: pymysql.connect, schema_filter: List[str] = []) -> Dict[str, Sequence]:
    seq_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    where_schema_filter = "where sequence_schema in (" + ",".join(
        list(map(lambda x: f"'{x}'", schema_filter))) + ")" if len(schema_filter) != 0 else ""
    try:
        cursor.execute(
            f"select sequence_schema,sequence_name,cycle,increment,max_value,min_value from information_schema.sequences {where_schema_filter};")
        for row in cursor.fetchall():
            try:
                # 处理可能为None的值
                cycle = 0 if row[2] is None else row[2]
                increment = 1 if row[3] is None else row[3]
                max_value = (1 << 63) - 1 if row[4] is None else row[4]
                min_value = 1 if row[5] is None else row[5]
                
                seq_map[row[0] + "." + row[1]] = Sequence(
                    sequence_schema=row[0],
                    sequence_name=row[1],
                    cycle=cycle,
                    increment=increment,
                    max_value=max_value,
                    min_value=min_value
                )
            except Exception as e:
                logging.error(f"Error processing sequence {row[0]}.{row[1]}: {str(e)}")
                continue
    except Exception as e:
        logging.error(f"Error querying sequences: {str(e)}")
    finally:
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


class Binding:
    def __init__(self, original_sql, bind_sql):
        self.original_sql = original_sql
        self.bind_sql = bind_sql

def get_binding_map(conn: pymysql.connect) -> Dict[str, Binding]:
    binding_map = {}
    cursor = conn.cursor(pymysql.cursors.Cursor)
    cursor.execute(
        f"select original_sql,bind_sql from mysql.bind_info where status='enabled' and default_db !='mysql' ;")
    for row in cursor.fetchall():
        binding_map[row[0]] = Binding(original_sql=row[0], bind_sql=row[1])
    cursor.close()
    return binding_map


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

class CustomHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """自定义帮助信息格式化器，同时显示默认值和必填标记"""
    def _get_help_string(self, action):
        # 先获取默认的帮助字符串（包含默认值信息）
        default_help = super()._get_help_string(action)
        
        # 如果是必填参数，在默认帮助文本前添加必填标记
        if action.required:
            return f'(必填) {default_help}'
        
        return default_help
    
def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(description="TiDB对象差异比较工具",
                                     formatter_class=CustomHelpFormatter)
    
    # 创建子命令解析器
    subparsers = parser.add_subparsers(title="Subcommands", dest="subcommand")
    
    # 添加check子命令
    parser_check = subparsers.add_parser("check", help="表结构对比检查")
    parser_check.add_argument('--src-host', help="上游IP地址", required=True)
    parser_check.add_argument('--tgt-host', help="下游IP地址", required=True)
    parser_check.add_argument('--tgt-port', help="端口号,默认4000", default=4000, type=int)
    parser_check.add_argument('--src-port', help="端口号,默认4000", default=4000, type=int)
    parser_check.add_argument('--user', '-u', help="用户名", default="root")
    parser_check.add_argument('--password', '-p', help="密码", nargs='?')
    parser_check.add_argument('--schema-list', '-s',
                            help="schema列表，指定多个用分隔符隔开，比如：db1,db2,db3，默认包含所有schema", 
                            default="*")

    # 添加dump-seq子命令
    parser_dumpseq = subparsers.add_parser("dump-seq", help="导出sequence",
                                           formatter_class=CustomHelpFormatter)
    parser_dumpseq.add_argument('-H', '--host', help="IP地址", required=True)
    # 添加一个目标端主机，用于做参数对比
    parser_dumpseq.add_argument('--tgt-host', help="IP地址", required=False)
    parser_dumpseq.add_argument('-P', '--port', help="端口号", default=4000, type=int)
    parser_dumpseq.add_argument('-u', '--user', help="用户名", default="root")
    parser_dumpseq.add_argument('-p', '--password', help="密码", nargs='?')
    parser_dumpseq.add_argument('--schema-list', '-s',
                             help="schema列表，指定多个用分隔符隔开，比如：db1,db2,db3，默认包含所有schema", 
                             default="*")
    
    return parser


def check(args):
    """执行check子命令"""
    if args.password is None:
        args.password = getpass.getpass()
    schema_filter = []
    if args.schema_list != "*":
        schema_filter = args.schema_list.split(",")
    src_connection = pymysql.connect(host=args.src_host, port=args.src_port, user=args.user,
                                   password=args.password)
    tgt_connection = pymysql.connect(host=args.tgt_host, port=args.tgt_port, user=args.user,
                                   password=args.password)
    src_index_map = get_index_map(src_connection, schema_filter)
    tgt_index_map = get_index_map(tgt_connection, schema_filter)
    src_table_map = get_simpltable_map(src_connection, schema_filter)
    tgt_table_map = get_simpltable_map(tgt_connection, schema_filter)
    src_user_map = get_user_map(src_connection)
    tgt_user_map = get_user_map(tgt_connection)
    src_sequence_map = get_sequence_map(src_connection, schema_filter)
    tgt_sequence_map = get_sequence_map(tgt_connection, schema_filter)
    src_constraints_map = get_constraints_map(src_connection, schema_filter)
    tgt_constraints_map = get_constraints_map(tgt_connection, schema_filter)
    src_variable_map = get_variable_map(src_connection)
    tgt_variable_map = get_variable_map(tgt_connection)
    src_binding_map = get_binding_map(src_connection)
    tgt_binding_map = get_binding_map(tgt_connection)

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
    k = "[INDEX]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_index_map, tgt_index_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    # 检查sequence
    logging.info("查看Sequence差异")
    k = "[SEQUENCE]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_sequence_map, tgt_sequence_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    # 检查binding
    logging.info("查看binding差异")
    k = "[BINDING]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_binding_map, tgt_binding_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    # 检查约束
    logging.info("查看约束差异")
    k = "[CONSTRAINTS]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_constraints_map, tgt_constraints_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    logging.info("查看用户差异")
    k = "[USER]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_user_map, tgt_user_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

    logging.info("查看重点参数差异")
    # 获取系统变量参数
    k = "[Variable]"
    v = ("source", "target", "difference")
    print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")
    for k, v in get_map_diff(src_variable_map, tgt_variable_map).items():
        print(f"{k:<{p1}}{v[0]:^{p2}}{v[1]:^{p3}}{v[2]:^{p4}}")

def dump_seq(args):
    """执行dump-seq子命令"""
    if args.password is None:
        args.password = getpass.getpass()
    schema_filter = []
    if args.schema_list != "*":
        schema_filter = args.schema_list.split(",")
    connection = pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password)
    tgt_connection = None
    if hasattr(args, 'tgt_host') and args.tgt_host:
        # 使用与源相同的端口，因为dump-seq命令没有单独的tgt_port参数
        tgt_connection = pymysql.connect(host=args.tgt_host, port=args.port, user=args.user, password=args.password)
    for v in dump_sequences_ddl(connection,tgt_connection, schema_filter).values():
        print(v)

def main():
    """主函数"""
    try:
        parser = create_parser()
        args = parser.parse_args()
        
        # Python 3.6 兼容：手动检查是否提供了子命令
        if not args.subcommand:
            parser.print_help()
            sys.exit(1)
            
        # 执行对应的子命令
        if args.subcommand == "check":
            check(args)
        elif args.subcommand == "dump-seq":
            dump_seq(args)
            
    except Exception as e:
        logging.error(f"执行出错: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

