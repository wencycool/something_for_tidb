#!/usr/bin/python
# encoding=utf8
import logging as log
import pymysql
import sqlite3
from tabulate import tabulate
import argparse
import getpass
import sys

# 判断python的版本
if sys.version_info < (3, 7):
    raise Exception("python version need larger than 3.6")

# 需要过滤的对比参数名称（variable和config参数名直接写在这里），支持类似于like语句的模糊匹配
ignore_vars = ["tidb_config", "%urls", "%path%", "%file%", "%addr%", "%log", "%dir", "%endpoints",
               "engine-store.flash.proxy.config", "initial-cluster", "socket", "port", "metric.job",
               "name"]


class TiDBInfo:
    def __init__(self, connection, sqlite3_filename="tidb-config"):
        """
        :param pymysql.connect connection: 连接信息
        :return:
        """
        self.conn = connection
        self.sqlite3_filename = sqlite3_filename

    @property
    def tidb_version(self):
        """
        :return str:返回数据库版本
        """
        cursor = self.conn.cursor()
        cursor.execute("select version()")
        version = cursor.fetchone()[0].split("-TiDB-")[1]
        cursor.close()
        return version

    def __init_cfg_table(self):
        """
        创建参数配置表并返回表名
        :return: 表名
        """
        host_info = self.conn.get_host_info().split()[1]
        host_info_format = host_info.replace(".", "_").replace(":", "__")
        cursor = sqlite3.connect(self.sqlite3_filename).cursor()
        version_format = self.tidb_version.replace(".", "_")
        table_name = "tidb_cfg_" + version_format + "_host_" + host_info_format
        delete_table_ddl = f"drop table if exists {table_name}"
        create_table_ddl = f"create table if not exists {table_name} (scope varchar(20),type varchar(20),var_name varchar(200),var_value text)"
        log.debug(f"create table ddl:{create_table_ddl}")
        cursor.execute(delete_table_ddl)
        cursor.execute(create_table_ddl)
        cursor.close()
        return table_name

    def get_cfg_tables(self):
        """
        获取配置文件列表
        :return list: 返回表列表
        """
        conn = sqlite3.connect(self.sqlite3_filename)
        tables = []
        for each_table in conn.execute(
                f"select tbl_name from sqlite_master where type='table' and tbl_name like 'tidb_cfg_%'").fetchall():
            tables.append(each_table[0])
        conn.close()
        return tables

    def has_table(self, tabname):
        conn = sqlite3.connect(self.sqlite3_filename)
        result = conn.execute(
            f"select count(*) as cnt from sqlite_master where type='table' and tbl_name='{tabname}'").fetchone()[0]
        conn.close()
        if result > 0:
            return True
        return False

    def report_diff(self, table1="", table2="", ignore_vars=[], auto=True, limit=0, offset=0):
        """
        比较两个参数表的差异
        :param auto: 是否自动获取最近2张表并进行差异判断
        :param ignore_vars: 忽略对比的参数项，不打印在该列表中的参数差异
        :param table1: 第一个参数表
        :param table2: 第二个参数表
        :param limit: 打印行数，0表示打印所有行，其它表示只输出前N行
        :return: 返回差异报表
        """
        if not self.has_table(table1) or not self.has_table(table2):
            if auto:
                tables = self.get_cfg_tables()
                if len(tables) <= 1:
                    tables_str = ",".join(tables)
                    raise Exception(f"系统表中表个数需大于1，当前表为:{tables_str}")
                else:
                    # table1, table2 = sorted(tables, reverse=True)[:2]
                    table1, table2 = tables[-1: -3: -1]
            else:
                if table1 == "" or table2 == "":
                    raise Exception(f"待比较的两个配置表必须存在")
        log.info(f"table1:{table1},table2:{table2}")
        ignore_scope = ["session"]  # 不显示为session级别变量
        table1_header = "_host_".join(
            map(lambda x: x.replace("_", "."), table1.removeprefix("tidb_cfg_").replace("__", ":").split("_host_")))
        table2_header = "_host_".join(
            map(lambda x: x.replace("_", "."), table2.removeprefix("tidb_cfg_").replace("__", ":").split("_host_")))
        headers = ["Number", "scope", "type", "var_name", f"var_value_{table1_header}", f"var_value_{table2_header}"]
        # 查找table1中有，table2中没有的参数
        data = []  # 和headers对齐
        except_table2_sql = (f"select scope,type,var_name,var_value from {table1} where (scope,type,var_name) in ("
                             f"select scope,type,var_name from {table1} except select scope,type,var_name from {table2}"
                             f")")
        sqlite3_conn = sqlite3.connect(self.sqlite3_filename)
        for row in sqlite3_conn.execute(except_table2_sql).fetchall():
            data.append([row[0], row[1], row[2], row[3], "NotFound"])
        # 查找table2中有，table1中没有的参数
        except_table1_sql = (f"select scope,type,var_name,var_value from {table2} where (scope,type,var_name) in ("
                             f"select scope,type,var_name from {table2} except select scope,type,var_name from {table1}"
                             f")")
        for row in sqlite3_conn.execute(except_table1_sql).fetchall():
            data.append([row[0], row[1], row[2], "NotFound", row[3]])
        # 查找table1和table2中都有但是参数值不同的参数
        intersect_sql = f"""select a.scope,a.type,a.var_name,b.var_value as {table1}_value,c.var_value as {table2}_value from 
                        (select scope,type,var_name from {table1}
                        INTERSECT
                        select scope,type,var_name from {table2}) a
                        left join {table1} b on a.scope=b.scope and a.type=b.type and a.var_name=b.var_name
                        left join {table2} c on a.scope=c.scope and a.type=c.type and a.var_name=c.var_name
                        where b.var_value != c.var_value"""
        for row in sqlite3_conn.execute(intersect_sql).fetchall():
            data.append([row[0], row[1], row[2], row[3], row[4]])
        # ignore_vars包含常规排查参数和模糊匹配形式，需找出模糊匹配形式，类似于like 支持 "%xx%","%xx","xx%"三种形式
        ignore_vars_expr = []
        ignore_vars_regular = []
        for var in ignore_vars:
            var_is_expr = False
            if var.startswith("%") and var.endswith("%"):
                ignore_vars_expr.append((1, var.removeprefix("%").removesuffix("%")))
                var_is_expr = True
            elif var.startswith("%"):
                ignore_vars_expr.append((2, var.removeprefix("%")))
                var_is_expr = True
            elif var.endswith("%"):
                ignore_vars_expr.append((3, var.removesuffix("%")))
                var_is_expr = True
            if not var_is_expr:
                ignore_vars_regular.append(var)
        for i in reversed(range(len(data))):
            row = data[i]
            # 删除不需要列
            if row[0] in ignore_scope:
                del data[i]
                continue
            if row[2] in ignore_vars_regular:
                del data[i]
                # 判断是否在表达式中
            else:
                for (t, var) in ignore_vars_expr:
                    # 处理包含逻辑
                    if t == 1 and var in row[2]:
                        del data[i]
                    # 处理开头包含逻辑
                    elif t == 2 and row[2].endswith(var):
                        del data[i]
                    # 处理结尾包含逻辑
                    elif t == 3 and row[2].startswith(var):
                        del data[i]
        data_with_number = []
        i = 0
        for row in data:
            i += 1
            new_row = [i] + row
            data_with_number.append(new_row)
        if limit > 0:
            data_with_number = data_with_number[offset:offset + limit]
        sqlite3_conn.close()
        return tabulate(data_with_number, headers=headers, tablefmt="simple_grid")

    def insert_tidb_vars(self):
        global sqlite3_conn
        table_name = self.__init_cfg_table()
        cursor = self.conn.cursor()
        insert_sql = f"insert into {table_name} (scope,type,var_name,var_value) values (?,?,?,?)"
        # 获取session级系统变量
        cursor.execute("show variables like 'tidb_%'")
        try:
            sqlite3_conn = sqlite3.connect(self.sqlite3_filename)
            sqlite3_conn.execute("begin")
            for row in cursor.fetchall():
                sqlite3_conn.execute(insert_sql, ('session', 'variable', row[0], row[1]))
            cursor.execute("show global variables like 'tidb_%'")
            for row in cursor.fetchall():
                sqlite3_conn.execute(insert_sql, ('global', 'variable', row[0], row[1]))
            # 获取show config信息
            cursor.execute("show config")
            # 进行去重
            var_name_map = {}
            for row in cursor.fetchall():
                key = (row[0], row[2])
                if key in var_name_map:
                    continue
                else:
                    var_name_map[key] = None
                # sqlite3_cursor.execute(insert_sql, ('global', row[0], row[2], row[3]))
                sqlite3_conn.execute(insert_sql, ('global', row[0], row[2], row[3]))
            sqlite3_conn.commit()
        except Exception as e:
            sqlite3_conn.rollback()
            log.warning(f"事务回滚:{e}")
        finally:
            sqlite3_conn.close()
        cursor.close()


def collect(args) -> TiDBInfo:
    if args.password is None:
        args.password = getpass.getpass("Enter your password:")
    try:
        connection = pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password,
                                     database="information_schema")
    except Exception as e:
        print(e)
        return
    tidbInfo = TiDBInfo(connection, args.db)
    tidbInfo.insert_tidb_vars()
    return tidbInfo


def report(args):
    tidbInfo = TiDBInfo(None, args.db)
    if args.list_tables:
        tables_str = ",".join(tidbInfo.get_cfg_tables())
        print(f"TABLE LIST:[{tables_str}]")
        return
    limit_list = args.limit.split(",")
    offset = 0
    limit = 0
    try:
        if len(limit_list) == 1:
            limit = int(limit_list[0])
        elif len(limit_list) == 2:
            limit = int(limit_list[1])
            offset = int(limit_list[0])
    except Exception as e:
        print(e)
        return
    print(
        tidbInfo.report_diff(args.table1, args.table2, ignore_vars=ignore_vars, auto=True, limit=limit, offset=offset))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="参数对比工具")
    parser.add_argument('-d', '--db', help="sqlite3的存放地址", default="tidb-config.db")
    subparsers = parser.add_subparsers(title="Subcommands", dest="subcommand", required=True)
    parser_collect = subparsers.add_parser("collect", help="搜集系统参数和集群参数")
    parser_collect.add_argument('-H', '--host', help="IP地址", default="127.0.0.1")
    parser_collect.add_argument('-P', '--port', help="端口号", default=4000, type=int)
    parser_collect.add_argument('-u', '--user', help="用户名", default="root")
    parser_collect.add_argument('-p', '--password', help="密码", nargs="?")

    parser_report = subparsers.add_parser("report", help="参数对比输出")
    parser_report.add_argument('-l', '--list-tables', action="store_true", help="打印当前已经完成采集的系统表")
    parser_report.add_argument('--table1', help="对比的第一个表", required=False)
    parser_report.add_argument('--table2', help="对比的第二个表", required=False)
    parser_report.add_argument('--limit', help="打印输出行数,默认输出所有行", default="0", type=str)
    args = parser.parse_args()
    # log.basicConfig(level=log.DEBUG)

    if args.subcommand == "collect":
        collect(args)
    elif args.subcommand == "report":
        report(args)
