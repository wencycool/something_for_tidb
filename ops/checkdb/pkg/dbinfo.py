import logging
from datetime import datetime, timedelta
import pymysql
from typing import List
import traceback
from .utils import set_max_memory
import sqlite3
from .duplicate_index import Index, get_tableindexes, CONST_DUPLICATE_INDEX, CONST_SUSPECTED_DUPLICATE_INDEX

from .utils import catch_exception
# 关键字，实例变量不能使用这些关键字
KEYWORDS = ["class_to_table_name", "fields"]
# 实例变量是字符串，如果值长度比较长，创建表结构时需要特殊处理
LONG_VARCHAR_TABLE_COLUMNS = ["plan", "query", "table_names", "index_names", "digest_text", "query_sample_text"]

# 从数据库获取的数据保存到sqlite3的数据表中
# 创建基类，用于生成建表语句和insert语句，让其它类继承
class BaseTable:
    def __init__(self):
        self.class_to_table_name = "tidb_" + self.__class__.__name__.lower()
        self.fields = {}
        # 字段排除这里基表定义的变量，以及系统变量
        for key, value in self.__dict__.items():
            if key in ["class_to_table_name", "fields"]:
                continue
            if isinstance(value, str):
                # 对超长字段做特殊处理
                if key in LONG_VARCHAR_TABLE_COLUMNS:
                    self.fields[key] = "text"
                else:
                    self.fields[key] = "varchar(512)"  # variable的optimizer_switch字段比较长，这里要支持
            elif isinstance(value, int):
                self.fields[key] = "int"
            elif isinstance(value, float):
                self.fields[key] = "float"
            elif isinstance(value, bool):
                self.fields[key] = "tinyint"
            elif isinstance(value, dict):
                self.fields[key] = "text"
            elif isinstance(value, list):
                self.fields[key] = "text"
            # 如果是自定义的类则打印__str__方法
            elif hasattr(value, "__str__"):
                self.fields[key] = "text"
            else:
                self.fields[key] = "datetime"

    def drop_table_sql(self):
        return f"drop table if exists {self.class_to_table_name}"

    def create_table_sql(self):
        sql = f"create table if not exists {self.class_to_table_name} ("
        for key, value in self.fields.items():
            sql += f"{key} {value},"
        sql = sql[:-1] + ")"
        return sql

    def insert_sql(self):
        """
        生成插入语句,默认插入所有字段，值为每实例变量的值
        :return:
        """
        sql = f"insert into {self.class_to_table_name} ("
        for key in self.fields.keys():
            sql += f"{key},"
        sql = sql[:-1] + ") values ("
        for key, value in self.__dict__.items():
            if key in ["class_to_table_name", "fields"]:
                continue
            if isinstance(value, str):
                # 对字符串进行转义
                v = value.replace("'", "''")
                sql += f"'{v}',"
            elif isinstance(value, bool):
                sql += f"{int(value)},"
            elif isinstance(value, list) or isinstance(value, dict):
                # 列表转为\n分隔的字符串
                v = ",".join(value).replace("'", "''")
                sql += f"'{v}',"
            elif hasattr(value, "__str__"):
                v = value.__str__().replace("'", "''")
                sql += f"'{v}',"
            else:
                sql += f"{value},"
        sql = sql[:-1] + ")"
        return sql


class DuplicateIndex(BaseTable, Index):
    def __init__(self):
        Index.__init__(self)
        BaseTable.__init__(self)


def get_duplicate_indexes(conn):
    """
    获取数据库中所有重复索引
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[DuplicateIndex]
    """
    duplicate_indexes = []
    table_indexes = get_tableindexes(conn)
    for table_index in table_indexes:
        table_index.analyze_indexes()
        for index in table_index.indexes:
            if index.state == CONST_DUPLICATE_INDEX or index.state == CONST_SUSPECTED_DUPLICATE_INDEX:
                duplicate_index = DuplicateIndex()
                duplicate_index.table_schema = table_index.table_schema
                duplicate_index.table_name = table_index.table_name
                duplicate_index.index_name = index.index_name
                duplicate_index.columns = index.columns
                duplicate_index.state = index.state
                duplicate_index.covered_by = index.covered_by
                duplicate_indexes.append(duplicate_index)
    return duplicate_indexes


class Variable(BaseTable):
    def __init__(self):
        self.type = ""  # 如果是系统参数则为variable,如果是集群参数则为：tidb,pd,tikv,tiflash
        self.name = ""
        self.value = ""
        super().__init__()


def get_variables(conn):
    """
    获取数据库中所有变量
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[Variable]
    """
    variables: List[Variable] = []
    cursor = conn.cursor()
    cursor.execute("show global variables")
    for row in cursor:
        variable = Variable()
        variable.type = "variable"
        variable.name = row[0]
        variable.value = row[1]
        variables.append(variable)
    cursor.execute("show config")
    # 过滤器，如果一个参数出现过则不再添加
    var_filter = {}
    for row in cursor:
        if (row[0], row[2]) in var_filter:
            continue
        var_filter[(row[0], row[2])] = True
        variable = Variable()
        variable.type = row[0]
        variable.name = row[2]
        variable.value = row[3]
        variables.append(variable)
    cursor.close()
    return variables


class ColumnCollation(BaseTable):
    def __init__(self):
        self.table_schema = ""
        self.table_name = ""
        self.column_name = ""
        self.collation_name = ""
        super().__init__()


def get_column_collations(conn):
    """
    获取数据库中所有列的排序规则
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[ColumnCollation]
    """
    collations: List[ColumnCollation] = []
    cursor = conn.cursor()
    cursor.execute(
        "select table_schema,table_name,column_name,collation_name from information_schema.columns where COLLATION_NAME !='utf8mb4_bin' and table_schema not in ('mysql','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA')")
    for row in cursor:
        collation = ColumnCollation()
        collation.table_schema = row[0]
        collation.table_name = row[1]
        collation.column_name = row[2]
        collation.collation_name = row[3]
        collations.append(collation)
    cursor.close()
    return collations


class UserPrivilege(BaseTable):
    def __init__(self):
        self.user = ""
        self.host = ""
        self.privilege: [str] = []  # 按照权限名称排序
        super().__init__()


def get_user_privileges(conn):
    """
    获取数据库中所有用户的权限
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[UserPrivilege]
    """
    privileges: List[UserPrivilege] = []
    cursor = conn.cursor()
    cursor.execute("select user,host from mysql.user where user !=''")
    for row in cursor:
        privilege = UserPrivilege()
        privilege.user = row[0]
        privilege.host = row[1]
        cursor_inner = conn.cursor()
        cursor_inner.execute(f"show grants for '{privilege.user}'@'{privilege.host if not None else '%'}'")
        for row_inner in cursor_inner:
            privilege.privilege.append(row_inner[0])
        cursor_inner.close()
        # 对privilege排序
        privilege.privilege.sort()
        privileges.append(privilege)
    cursor.close()
    return privileges


class VersionNotMatchError(Exception):
    def __init__(self, message):
        self.message = message


class NodeVersion(BaseTable):
    """
    节点版本信息,如果集群中各节点版本不一致，则抛出异常，如果同一节点类型的git_hash不一致，则抛出异常
    """

    def __init__(self):
        self.node_type = ""  # 节点类型
        self.version = ""  # 版本号
        self.git_hash = ""  # git hash，用于判断补丁版本
        super().__init__()


def get_node_versions(conn):
    """
    获取数据库中所有节点的版本信息
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[NodeVersion]
    """
    versions: List[NodeVersion] = []
    all_node_versions = []
    cursor = conn.cursor()
    cursor.execute("select type,version,git_hash from information_schema.cluster_info")
    for row in cursor:
        version = NodeVersion()
        version.node_type = row[0]
        version.version = row[1]
        version.git_hash = row[2]
        all_node_versions.append(version)
    cursor.close()
    # 检查版本是否一致
    version_map = {}
    base_version = ""
    for version in all_node_versions:
        if base_version == "":
            base_version = version.version
        if base_version != version.version:
            raise VersionNotMatchError("Version not match")
        if version.node_type not in version_map:
            version_map[version.node_type] = version
        else:
            # 对同一类型节点，检查git hash是否一致
            if version_map[version.node_type].git_hash != version.git_hash:
                raise VersionNotMatchError(f"Git hash not match for {version.node_type}")
    for version in all_node_versions:
        versions.append(version)
    return versions


class SlowQuery(BaseTable):
    def __init__(self):
        self.digest = ""
        self.exec_count = 0
        self.avg_query_time = 0
        self.succ_count = 0
        self.sum_query_time = 0
        self.sum_total_keys = 0
        self.avg_total_keys = 0
        self.sum_process_keys = 0
        self.avg_process_keys = 0
        self.first_seen = ""
        self.last_seen = ""
        self.mem_max = 0
        self.disk_max = 0
        self.avg_result_rows = 0
        self.max_result_rows = 0
        self.plan_from_binding = 0
        self.plan_digest = ""
        self.query = ""
        self.plan = ""
        super().__init__()


# 获取慢查询信息,默认查询最近一天的慢查询
def get_slow_query_info(conn, start_time=None, end_time=None):
    """
    获取慢查询信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :param start_time: 慢查询开始时间
    :type start_time: datetime
    :param end_time: 慢查询结束时间
    :type end_time: datetime
    :rtype: List[SlowQuery]
    """
    # mysql> select time from information_schema.cluster_slow_query limit 1;
    # +----------------------------+
    # | time                       |
    # +----------------------------+
    # | 2024-08-01 20:40:08.948763 |
    # +----------------------------+
    # 1 row in set (0.01 sec)
    # 将时间转换为字符串用于SQL查询
    if not start_time or not end_time or start_time >= end_time:
        # 查询最近一天的慢查询
        start_time_str = "adddate(now(),INTERVAL -1 DAY)"
        end_time_str = "now()"
    else:
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    slow_queries = []
    # get from https://tidb.net/blog/90e27aa0
    # Binary_plan在v6.5才开始引入，所以这里不做处理
    slow_query_sql = f"""
    WITH ss AS
    (SELECT s.Digest ,s.Plan_digest,
    count(1) exec_count,
    sum(s.Succ) succ_count,
    round(sum(s.Query_time),4) sum_query_time,
    round(avg(s.Query_time),4) avg_query_time,
    sum(s.Total_keys) sum_total_keys,
    avg(s.Total_keys) avg_total_keys,
    sum(s.Process_keys) sum_process_keys,
    avg(s.Process_keys) avg_process_keys,
    min(s.`Time`) min_time,
    max(s.`Time`) max_time,
    round(max(s.Mem_max)/1024/1024,4) Mem_max,
    round(max(s.Disk_max)/1024/1024,4) Disk_max,
    avg(s.Result_rows) avg_Result_rows,
    max(s.Result_rows) max_Result_rows,
    sum(Plan_from_binding) Plan_from_binding
    FROM information_schema.cluster_slow_query s
    WHERE s.time>='{start_time_str}'
    AND s.time<= '{end_time_str}'
    AND s.Is_internal =0
    -- AND UPPER(s.query) NOT LIKE '%ANALYZE TABLE%'
    -- AND UPPER(s.query) NOT LIKE '%DBEAVER%'
    -- AND UPPER(s.query) NOT LIKE '%ADD INDEX%'
    -- AND UPPER(s.query) NOT LIKE '%CREATE INDEX%'
    GROUP BY s.Digest ,s.Plan_digest
    ORDER BY sum(s.Query_time) desc
    LIMIT 35)
    SELECT /*+ MAX_EXECUTION_TIME(30000) MEMORY_QUOTA(2048 MB) */
    ss.Digest,         -- SQL Digest
    ss.Plan_digest,           -- PLAN Digest
    (SELECT s1.Query FROM information_schema.cluster_slow_query s1 WHERE s1.Digest=ss.digest AND s1.time>=ss.min_time AND s1.time<=ss.max_time LIMIT 1) query,  -- SQL文本
    (SELECT s2.plan FROM information_schema.cluster_slow_query s2 WHERE s2.Plan_digest=ss.plan_digest AND s2.time>=ss.min_time AND s2.time<=ss.max_time LIMIT 1) plan, -- 执行计划
    ss.exec_count,            -- SQL总执行次数
    ss.succ_count,            -- SQL执行成功次数
    ss.sum_query_time,        -- 总执行时间（秒）
    ss.avg_query_time,        -- 平均单次执行时间（秒）
    ss.sum_total_keys,        -- 总扫描key数量
    ss.avg_total_keys,        -- 平均单次扫描key数量
    ss.sum_process_keys,      -- 总处理key数量
    ss.avg_process_keys,      -- 平均单次处理key数量
    ss.min_time,              -- 查询时间段内第一次SQL执行结束时间
    ss.max_time,              -- 查询时间段内最后一次SQL执行结束时间
    ss.Mem_max,               -- 单次执行中内存占用最大值（MB）
    ss.Disk_max,              -- 单次执行中磁盘占用最大值（MB）
    ss.avg_Result_rows,       -- 平均返回行数
    ss.max_Result_rows,       -- 单次最大返回行数
    ss.Plan_from_binding      -- 走SQL binding的次数
    FROM ss;
    """
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        cursor.execute(slow_query_sql)
    except Exception as e:
        logging.error(f"Get slow query failed: {e}, {traceback.format_exc()}")
        return slow_queries
    for row in cursor:
        slow_query = SlowQuery()
        slow_query.digest = row["Digest"]
        slow_query.plan_digest = row["Plan_digest"]
        slow_query.query = row["query"]
        slow_query.plan = row["plan"]
        slow_query.exec_count = row["exec_count"]
        slow_query.succ_count = row["succ_count"]
        slow_query.sum_query_time = row["sum_query_time"]
        slow_query.avg_query_time = row["avg_query_time"]
        slow_query.sum_total_keys = row["sum_total_keys"]
        slow_query.avg_total_keys = row["avg_total_keys"]
        slow_query.sum_process_keys = row["sum_process_keys"]
        slow_query.avg_process_keys = row["avg_process_keys"]
        slow_query.first_seen = row["min_time"]
        slow_query.last_seen = row["max_time"]
        slow_query.mem_max = row["Mem_max"]
        slow_query.disk_max = row["Disk_max"]
        slow_query.avg_result_rows = row["avg_Result_rows"]
        slow_query.max_result_rows = row["max_Result_rows"]
        slow_query.plan_from_binding = row["Plan_from_binding"]
        slow_queries.append(slow_query)
    cursor.close()
    return slow_queries


class StatementHistory(BaseTable):
    def __init__(self):
        self.digest = ""
        self.exec_count = 0
        self.stmt_type = ""
        self.avg_latency = 0
        self.instance = ""
        self.summary_begin_time = ""
        self.summary_end_time = ""
        self.first_seen = ""
        self.last_seen = ""
        self.plan_digest = ""
        self.sum_latency = 0
        self.avg_mem = 0
        self.avg_disk = 0
        self.avg_result_rows = 0
        self.avg_affected_rows = 0
        self.avg_processed_keys = 0
        self.avg_total_keys = 0
        self.avg_rocksdb_delete_skipped_count = 0
        self.avg_rocksdb_key_skipped_count = 0
        self.avg_rocksdb_block_read_count = 0
        self.schema_name = ""
        self.table_names = ""
        self.index_names = ""
        self.digest_text = ""
        self.query_sample_text = ""
        self.prev_sample_text = ""
        self.plan = ""
        super().__init__()


# 查询当前数据库中INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY表数据
def get_statement_history(conn, min_latency=50):
    """
    获取数据库中INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY视图中的SQL
    :param min_latency: 高于该值的SQL才会被返回，单位：毫秒
    :type min_latency: int
    :param conn: pymysql.connections.Connection
    :type conn: pymysql.connections.Connection
    :return: List[StatementHistory]
    """
    statement_histories = []
    statement_history_sql = f"""
    with top_sql as (select *
                 from (select *, row_number() over(partition by INSTANCE,SUMMARY_BEGIN_TIME order by EXEC_COUNT desc) as nbr
                       from INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY
                       where AVG_LATENCY/1000000 >= {min_latency}) a -- 超过50ms的SQL
                 where a.nbr <= 30) -- 取每个批次的前30条SQL

    select /*+ MAX_EXECUTION_TIME(10000) MEMORY_QUOTA(1024 MB) */ EXEC_COUNT,STMT_TYPE,round(AVG_LATENCY/1000000000,3) as AVG_LATENCY,INSTANCE,SUMMARY_BEGIN_TIME,SUMMARY_END_TIME,FIRST_SEEN,LAST_SEEN,DIGEST,PLAN_DIGEST,round(SUM_LATENCY/1000000000,3) as SUM_LATENCY,AVG_MEM,AVG_DISK,AVG_RESULT_ROWS,AVG_AFFECTED_ROWS,AVG_PROCESSED_KEYS,AVG_TOTAL_KEYS,AVG_ROCKSDB_DELETE_SKIPPED_COUNT,AVG_ROCKSDB_KEY_SKIPPED_COUNT,AVG_ROCKSDB_BLOCK_READ_COUNT,SCHEMA_NAME,TABLE_NAMES,INDEX_NAMES,DIGEST_TEXT,QUERY_SAMPLE_TEXT,PREV_SAMPLE_TEXT,PLAN
    from top_sql limit 100000 -- 控制最多返回10万条
    """
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        cursor.execute(statement_history_sql)
    except Exception as e:
        logging.error(f"Get statement history failed: {e}, {traceback.format_exc()}")
        return statement_histories
    for row in cursor:
        statement_history = StatementHistory()
        statement_history.exec_count = row["EXEC_COUNT"]
        statement_history.stmt_type = row["STMT_TYPE"]
        statement_history.avg_latency = row["AVG_LATENCY"]
        statement_history.instance = row["INSTANCE"]
        statement_history.summary_begin_time = row["SUMMARY_BEGIN_TIME"]
        statement_history.summary_end_time = row["SUMMARY_END_TIME"]
        statement_history.first_seen = row["FIRST_SEEN"]
        statement_history.last_seen = row["LAST_SEEN"]
        statement_history.digest = row["DIGEST"]
        statement_history.plan_digest = row["PLAN_DIGEST"]
        statement_history.sum_latency = row["SUM_LATENCY"]
        statement_history.avg_mem = row["AVG_MEM"]
        statement_history.avg_disk = row["AVG_DISK"]
        statement_history.avg_result_rows = row["AVG_RESULT_ROWS"]
        statement_history.avg_affected_rows = row["AVG_AFFECTED_ROWS"]
        statement_history.avg_processed_keys = row["AVG_PROCESSED_KEYS"]
        statement_history.avg_total_keys = row["AVG_TOTAL_KEYS"]
        statement_history.avg_rocksdb_delete_skipped_count = row["AVG_ROCKSDB_DELETE_SKIPPED_COUNT"]
        statement_history.avg_rocksdb_key_skipped_count = row["AVG_ROCKSDB_KEY_SKIPPED_COUNT"]
        statement_history.avg_rocksdb_block_read_count = row["AVG_ROCKSDB_BLOCK_READ_COUNT"]
        statement_history.schema_name = row["SCHEMA_NAME"]
        statement_history.table_names = row["TABLE_NAMES"]
        statement_history.index_names = row["INDEX_NAMES"]
        statement_history.digest_text = row["DIGEST_TEXT"]
        statement_history.query_sample_text = row["QUERY_SAMPLE_TEXT"]
        statement_history.prev_sample_text = row["PREV_SAMPLE_TEXT"]
        statement_history.plan = row["PLAN"]
        statement_histories.append(statement_history)
    cursor.close()
    return statement_histories

# 获取集群节点信息
# -- 以节点为视角查询集群所有节点信息,包括端口号信息
# select type,instance,STATUS_ADDRESS,version,START_TIME,uptime,SERVER_ID from INFORMATION_SCHEMA.CLUSTER_INFO;
class NodeInfo(BaseTable):
    def __init__(self):
        self.type = ""
        self.instance = ""
        self.status_address = ""
        self.version = ""
        self.start_time = ""
        self.uptime = ""
        self.server_id = 0
        super().__init__()

def get_node_info(conn):
    """
    获取数据库中所有节点的信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[NodeInfo]
    """
    node_infos: List[NodeInfo] = []
    cursor = conn.cursor()
    cursor.execute("select type,instance,STATUS_ADDRESS,version,START_TIME,uptime,SERVER_ID from INFORMATION_SCHEMA.CLUSTER_INFO")
    for row in cursor:
        node_info = NodeInfo()
        node_info.type = row[0]
        node_info.instance = row[1]
        node_info.status_address = row[2]
        node_info.version = row[3]
        node_info.start_time = row[4]
        node_info.uptime = row[5]
        node_info.server_id = row[6]
        node_infos.append(node_info)
    cursor.close()
    return node_infos


# 将所有的函数输出写到sqlite3的数据表中

def SaveData(conn, callback, *args, **kwargs):
    """
    将所有的函数输出写到sqlite3的数据表中
    :param conn: 写入的数据库连接
    :type conn: sqlite3.Connection
    :param callback: 回调函数
    :type callback: Callable[[pymysql.connections.Connection], List[BaseTable]]
    :param args: callback的参数
    :type args: Any
    :param kwargs: callback的参数
    :type kwargs: Any
    :rtype: bool
    """
    try:
        rows = callback(*args, **kwargs)
        logging.debug(f"Get data from callback[{callback.__name__}]: {len(rows)}")
        cursor = conn.cursor()
        table_created = False
        # 500条数据一次提交
        batch_size = 500
        for (i, row) in enumerate(rows):
            try:
                if not table_created:
                    # logging.debug(f"Create table sql: {create_table_sql}")
                    cursor.execute(row.drop_table_sql())
                    cursor.execute(row.create_table_sql())
                    table_created = True
                cursor.execute(row.insert_sql())
                if i != 0 and i % batch_size == 0:
                    # logging.debug(f"insert sql: {row.insert_sql()}")
                    conn.commit()
            except Exception as e:
                logging.error(
                    f"Save data failed: {e}, {traceback.format_exc()}, {row.create_table_sql()}, {row.insert_sql()}")
                return False
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        logging.error(f"Save data failed: {e}, {traceback.format_exc()}")
        return False

# -- 以os为视角，查询集群中所有主机信息，包括如下属性：
# -- 1、CPU核数，内存大小
# WITH ip_node_map as (SELECT ip_address,
#                             GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
#                      FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
#                                   TYPE,
#                                   COUNT(*)                          AS TYPE_COUNT
#                            FROM INFORMATION_SCHEMA.CLUSTER_INFO
#                            GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
#                      GROUP BY ip_address),
#      os_info AS (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1)                         AS IP_ADDRESS,
#                         MAX(CASE WHEN NAME = 'cpu-physical-cores' THEN VALUE END) AS CPU_CORES,
#                         MAX(CASE WHEN NAME = 'capacity' THEN VALUE END)           AS MEMORY_CAPACITY,
#                         MAX(CASE WHEN NAME = 'cpu-arch' THEN VALUE END)           AS CPU_ARCH
#                  FROM INFORMATION_SCHEMA.CLUSTER_HARDWARE
#                  WHERE (DEVICE_TYPE = 'cpu' AND DEVICE_NAME = 'cpu' AND NAME IN ('cpu-arch', 'cpu-physical-cores'))
#                     OR (DEVICE_TYPE = 'memory' AND DEVICE_NAME = 'memory' AND NAME = 'capacity')
#                  GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1)),
#      ip_hostname_map as (select substring_index(instance, ':', 1) as ip_address,
#                                 value                             as hostname
#                          from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
#                          where name = 'kernel.hostname'
#                          group by ip_address, hostname)
# select c.hostname,
#        a.ip_address,
#        types_count,
#        cpu_arch,
#        cpu_cores,
#        round(memory_capacity / 1024 / 1024 / 1024, 1) as memory_capacity_gb
# from ip_node_map a
#          join os_info b
#               on a.ip_address = b.ip_address
#          join ip_hostname_map c on a.ip_address = c.ip_address;
class OSInfo(BaseTable):
    def __init__(self):
        self.hostname = ""
        self.ip_address = ""
        self.types_count = ""
        self.cpu_arch = ""
        self.cpu_cores = 0
        self.memory_capacity_gb = 0.0
        super().__init__()

def get_os_info(conn):
    """
    获取数据库中所有节点的操作系统信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[OSInfo]
    """
    os_infos: List[OSInfo] = []
    cursor = conn.cursor()
    cursor.execute("""
    WITH ip_node_map as (SELECT ip_address,
                                GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
                         FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
                                      TYPE,
                                      COUNT(*)                          AS TYPE_COUNT
                               FROM INFORMATION_SCHEMA.CLUSTER_INFO
                               GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
                         GROUP BY ip_address),
         os_info AS (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1)                         AS IP_ADDRESS,
                            MAX(CASE WHEN NAME = 'cpu-physical-cores' THEN VALUE END) AS CPU_CORES,
                            MAX(CASE WHEN NAME = 'capacity' THEN VALUE END)           AS MEMORY_CAPACITY,
                            MAX(CASE WHEN NAME = 'cpu-arch' THEN VALUE END)           AS CPU_ARCH
                     FROM INFORMATION_SCHEMA.CLUSTER_HARDWARE
                     WHERE (DEVICE_TYPE = 'cpu' AND DEVICE_NAME = 'cpu' AND NAME IN ('cpu-arch', 'cpu-physical-cores'))
                        OR (DEVICE_TYPE = 'memory' AND DEVICE_NAME = 'memory' AND NAME = 'capacity')
                     GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1)),
         ip_hostname_map as (select substring_index(instance, ':', 1) as ip_address,
                                    value                             as hostname
                             from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
                             where name = 'kernel.hostname'
                             group by ip_address, hostname)
    select c.hostname,
           a.ip_address,
           types_count,
           cpu_arch,
           cpu_cores,
           round(memory_capacity / 1024 / 1024 / 1024, 1) as memory_capacity_gb
    from ip_node_map a
             join os_info b
                  on a.ip_address = b.ip_address
             join ip_hostname_map c on a.ip_address = c.ip_address;
    """)
    for row in cursor:
        os_info = OSInfo()
        os_info.hostname = row[0]
        os_info.ip_address = row[1]
        os_info.types_count = row[2]
        os_info.cpu_arch = row[3]
        os_info.cpu_cores = row[4]
        os_info.memory_capacity_gb = row[5]
        os_infos.append(os_info)
    cursor.close()
    return os_infos

# -- 查看磁盘使用率情况
# with disk_info as (select a.time,
#                           a.device,
#                           a.instance,
#                           substring_index(a.instance, ':', 1)     as ip_address,
#                           a.fstype,
#                           a.mountpoint,
#                           round(a.value / 1024 / 1024 / 1024, 2)  as aval_size_gb,
#                           round(a.value / 1024 / 1024 / 1024, 2)  as total_size_gb,
#                           round((b.value - a.value) / b.value, 2) as used_percent
#                    from METRICS_SCHEMA.node_disk_available_size a,
#                         METRICS_SCHEMA.node_disk_size b
#                    where a.time = b.time
#                      and a.instance = b.instance
#                      and a.device = b.device
#                      and a.mountpoint = b.mountpoint
#                      and a.time = now()
#                      and a.mountpoint like '%%'),
#      ip_host_map as (select substring_index(instance, ':', 1) as ip_address,
#                             value                             as hostname
#                      from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
#                      where name = 'kernel.hostname'
#                      group by ip_address, hostname),
#      ip_node_map as (SELECT ip_address,
#                             GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
#                      FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
#                                   TYPE,
#                                   COUNT(*)                          AS TYPE_COUNT
#                            FROM INFORMATION_SCHEMA.CLUSTER_INFO
#                            GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
#                      GROUP BY ip_address)
# select a.time,
#        a.ip_address,
#        b.hostname,
#        c.TYPES_COUNT,
#        a.fstype,
#        a.mountpoint,
#        a.aval_size_gb,
#        a.total_size_gb,
#        a.used_percent
# from disk_info a
#          left join ip_host_map b on a.ip_address = b.ip_address
#          left join ip_node_map c on a.ip_address = c.ip_address
# order by a.time, a.device, a.instance;
class DiskInfo(BaseTable):
    def __init__(self):
        self.time = ""
        self.ip_address = ""
        self.hostname = ""
        self.types_count = ""
        self.fstype = ""
        self.mountpoint = ""
        self.aval_size_gb = 0.0
        self.total_size_gb = 0.0
        self.used_percent = 0.0
        super().__init__()

def get_disk_info(conn):
    """
    获取数据库中所有节点的磁盘使用率信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[DiskInfo]
    """
    disk_infos: List[DiskInfo] = []
    cursor = conn.cursor()
    cursor.execute("""
    with disk_info as (select a.time,
                              a.device,
                              a.instance,
                              substring_index(a.instance, ':', 1)     as ip_address,
                              a.fstype,
                              a.mountpoint,
                              round(a.value / 1024 / 1024 / 1024, 2)  as aval_size_gb,
                              round(a.value / 1024 / 1024 / 1024, 2)  as total_size_gb,
                              round((b.value - a.value) / b.value, 2) as used_percent
                       from METRICS_SCHEMA.node_disk_available_size a,
                            METRICS_SCHEMA.node_disk_size b
                       where a.time = b.time
                         and a.instance = b.instance
                         and a.device = b.device
                         and a.mountpoint = b.mountpoint
                         and a.time = now()
                         and a.mountpoint like '%%'),
         ip_host_map as (select substring_index(instance, ':', 1) as ip_address,
                                value                             as hostname
                         from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
                         where name = 'kernel.hostname'
                         group by ip_address, hostname),
         ip_node_map as (SELECT ip_address,
                                GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
                         FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
                                      TYPE,
                                      COUNT(*)                          AS TYPE_COUNT
                               FROM INFORMATION_SCHEMA.CLUSTER_INFO
                               GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
                         GROUP BY ip_address)
    select a.time,
           a.ip_address,
           b.hostname,
           c.TYPES_COUNT,
           a.fstype,
           a.mountpoint,
           a.aval_size_gb,
           a.total_size_gb,
           a.used_percent
    from disk_info a
             left join ip_host_map b on a.ip_address = b.ip_address
             left join ip_node_map c on a.ip_address = c.ip_address
    order by a.time, a.device, a.instance;
    """)
    for row in cursor:
        disk_info = DiskInfo()
        disk_info.time = row[0]
        disk_info.ip_address = row[1]
        disk_info.hostname = row[2]
        disk_info.types_count = row[3]
        disk_info.fstype = row[4]
        disk_info.mountpoint = row[5]
        disk_info.aval_size_gb = row[6]
        disk_info.total_size_gb = row[7]
        disk_info.used_percent = row[8]
        disk_infos.append(disk_info)
    cursor.close()
    return disk_infos

# select TABLE_SCHEMA,TABLE_NAME, table_rows,avg_row_length as avg_row_length_byte,round((DATA_LENGTH + INDEX_LENGTH) / 1024/1024/1024,2) as table_size_gb from INFORMATION_SCHEMA.tables where table_type='BASE TABLE' and (DATA_LENGTH + INDEX_LENGTH) / 1024/1024/1024 > 10 or  table_rows > 5000000;
class TableInfo(BaseTable):
    def __init__(self):
        self.table_schema = ""
        self.table_name = ""
        self.table_rows = 0
        self.avg_row_length_byte = 0
        self.table_size_gb = 0.0
        super().__init__()

def get_table_info(conn):
    """
    获取数据库中所有表的信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[TableInfo]
    """
    table_infos: List[TableInfo] = []
    cursor = conn.cursor()
    cursor.execute("""
    select TABLE_SCHEMA,TABLE_NAME, table_rows,avg_row_length as avg_row_length_byte,round((DATA_LENGTH + INDEX_LENGTH) / 1024/1024/1024,2) as table_size_gb from INFORMATION_SCHEMA.tables where table_type='BASE TABLE' and (DATA_LENGTH + INDEX_LENGTH) / 1024/1024/1024 > 10 or  table_rows > 5000000;
    """)
    for row in cursor:
        table_info = TableInfo()
        table_info.table_schema = row[0]
        table_info.table_name = row[1]
        table_info.table_rows = row[2]
        table_info.avg_row_length_byte = row[3]
        table_info.table_size_gb = row[4]
        table_infos.append(table_info)
    cursor.close()
    return table_infos

# -- 数据库内存增长率，只查看最近1周的各os内存增长率情况，每小时打印一次
# set @@tidb_metric_query_step = 3600;
# set @@tidb_metric_query_range_duration = 30;
# WITH ip_node_map as (SELECT ip_address,
#                             GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
#                      FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
#                                   TYPE,
#                                   COUNT(*)                          AS TYPE_COUNT
#                            FROM INFORMATION_SCHEMA.CLUSTER_INFO
#                            GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
#                      GROUP BY ip_address),
#      os_info AS (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1)                         AS IP_ADDRESS,
#                         MAX(CASE WHEN NAME = 'cpu-physical-cores' THEN VALUE END) AS CPU_CORES,
#                         MAX(CASE WHEN NAME = 'capacity' THEN VALUE END)           AS MEMORY_CAPACITY,
#                         MAX(CASE WHEN NAME = 'cpu-arch' THEN VALUE END)           AS CPU_ARCH
#                  FROM INFORMATION_SCHEMA.CLUSTER_HARDWARE
#                  WHERE (DEVICE_TYPE = 'cpu' AND DEVICE_NAME = 'cpu' AND NAME IN ('cpu-arch', 'cpu-physical-cores'))
#                     OR (DEVICE_TYPE = 'memory' AND DEVICE_NAME = 'memory' AND NAME = 'capacity')
#                  GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1)),
#      ip_hostname_map as (select substring_index(instance, ':', 1) as ip_address,
#                                 value                             as hostname
#                          from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
#                          where name = 'kernel.hostname'
#                          group by ip_address, hostname)
# select time,
#        substring_index(instance, ':', 1) as ip_address,
#        c.hostname,
#        b.TYPES_COUNT,
#        round(value / 100, 2)             as used_percent
# from METRICS_SCHEMA.node_memory_usage a
#          join ip_node_map b on substring_index(instance, ':', 1) = b.ip_address
#          join ip_hostname_map c on substring_index(instance, ':', 1) = c.ip_address
# where a.time between date_sub(now(), interval 7 day) and now();
class MemoryUsageDetail(BaseTable):
    def __init__(self):
        self.time = ""
        self.ip_address = ""
        self.hostname = ""
        self.types_count = ""
        self.used_percent = 0.0
        super().__init__()

def get_memory_detail(conn):
    """
    获取数据库中所有节点的内存使用率信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[MemoryUsageDetail]
    """
    memory_infos: List[MemoryUsageDetail] = []
    cursor = conn.cursor()
    cursor.execute("""
    WITH ip_node_map as (SELECT ip_address,
                                GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
                         FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
                                      TYPE,
                                      COUNT(*)                          AS TYPE_COUNT
                               FROM INFORMATION_SCHEMA.CLUSTER_INFO
                               GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
                         GROUP BY ip_address),
         os_info AS (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1)                         AS IP_ADDRESS,
                            MAX(CASE WHEN NAME = 'cpu-physical-cores' THEN VALUE END) AS CPU_CORES,
                            MAX(CASE WHEN NAME = 'capacity' THEN VALUE END)           AS MEMORY_CAPACITY,
                            MAX(CASE WHEN NAME = 'cpu-arch' THEN VALUE END)           AS CPU_ARCH
                     FROM INFORMATION_SCHEMA.CLUSTER_HARDWARE
                     WHERE (DEVICE_TYPE = 'cpu' AND DEVICE_NAME = 'cpu' AND NAME IN ('cpu-arch', 'cpu-physical-cores'))
                        OR (DEVICE_TYPE = 'memory' AND DEVICE_NAME = 'memory' AND NAME = 'capacity')
                     GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1)),
         ip_hostname_map as (select substring_index(instance, ':', 1) as ip_address,
                                value                             as hostname
                         from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
                         where name = 'kernel.hostname'
                         group by ip_address, hostname)
    select time,
           substring_index(instance, ':', 1) as ip_address,
           c.hostname,
           b.TYPES_COUNT,
           round(value / 100, 2)             as used_percent
    from METRICS_SCHEMA.node_memory_usage a
             join ip_node_map b on substring_index(instance, ':', 1) = b.ip_address
             join ip_hostname_map c on substring_index(instance, ':', 1) = c.ip_address
    where a.time between date_sub(now(), interval 7 day) and now();
    """)
    for row in cursor:
        memory_info = MemoryUsageDetail()
        memory_info.time = row[0]
        memory_info.ip_address = row[1]
        memory_info.hostname = row[2]
        memory_info.types_count = row[3]
        memory_info.used_percent = row[4]
        memory_infos.append(memory_info)
    cursor.close()
    return memory_infos

# -- 下面语句统计每个节点的，连接数总量、活跃连接数，对于整个集群的只需要汇总即可
# select b.type, b.hostname, a.instance, a.connection_count,a.active_connection_count
# from (select instance,
#              count(*) as connection_count,
#              sum(case when COMMAND !='Sleep' then 1 else 0 end) as active_connection_count
#       from INFORMATION_SCHEMA.CLUSTER_PROCESSLIST
#       group by instance) a
#          left join(select a.type,
#                           a.INSTANCE,
#                           a.value                                                   as hostname,
#                           concat(substring_index(a.INSTANCE, ':', 1), ':', b.value) as new_instance
#                    from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO a,
#                         INFORMATION_SCHEMA.CLUSTER_CONFIG b
#                    where a.type = 'tidb'
#                      and a.SYSTEM_TYPE = 'system'
#                      and a.SYSTEM_NAME = 'sysctl'
#                      and a.name = 'kernel.hostname'
#                      and a.INSTANCE = b.INSTANCE
#                      and b.`key` = 'status.status-port') b on a.INSTANCE = b.new_instance;
#
# -- 查看每个节点的连接数配置情况
# select type,
#        hostname,
#        report_instance as instance,
#        conns           as connection_count,
#        max_conns       as configured_max_counnection_count,
#        conn_ratio      as connection_ratio
# from (select b.type,
#              b.hostname,
#              a.instance                                                                 as report_instance,
#              b.instance,
#              a.conns,
#              c.max_conns,
#              case when c.max_conns <= 0 then 0 else round(a.conns / c.max_conns, 2) end as conn_ratio
#       from (select instance, cast(value as signed) as conns
#             from METRICS_SCHEMA.tidb_connection_count
#             where time = NOW()) a
#                left join(select a.type,
#                                 a.instance,
#                                 a.value                                                   as hostname,
#                                 concat(substring_index(a.instance, ':', 1), ':', b.value) as new_instance
#                          from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO a,
#                               INFORMATION_SCHEMA.CLUSTER_CONFIG b
#                          where a.type = 'tidb'
#                            and a.SYSTEM_TYPE = 'system'
#                            and a.SYSTEM_NAME = 'sysctl'
#                            and a.name = 'kernel.hostname'
#                            and a.instance = b.INSTANCE
#                            and b.`key` = 'status.status-port') b on a.instance = b.new_instance
#                left join (select row_number() over (partition by instance) as nbr,
#                                  instance,
#                                  cast(value as signed)                     as max_conns
#                           from INFORMATION_SCHEMA.CLUSTER_CONFIG
#                           where `key` in ('max-server-connections', 'instance.max_connections')) c
#                          on b.INSTANCE = c.INSTANCE and c.nbr = 1) a;
class ConnectionInfo(BaseTable):
    def __init__(self):
        self.type = ""
        self.hostname = ""
        self.instance = ""
        self.connection_count = 0
        self.active_connection_count = 0
        self.configured_max_counnection_count = 0
        self.connection_ratio = 0.0
        super().__init__()

def get_connection_info(conn):
    """
    获取数据库中所有节点的连接数信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[ConnectionInfo]
    """
    connection_infos: List[ConnectionInfo] = []
    cursor = conn.cursor()
    cursor.execute("""
    select b.type, b.hostname, a.instance, a.connection_count,a.active_connection_count
    from (select instance,
                 count(*) as connection_count,
                 sum(case when COMMAND !='Sleep' then 1 else 0 end) as active_connection_count
          from INFORMATION_SCHEMA.CLUSTER_PROCESSLIST
          group by instance) a
             left join(select a.type,
                              a.INSTANCE,
                              a.value                                                   as hostname,
                              concat(substring_index(a.INSTANCE, ':', 1), ':', b.value) as new_instance
                       from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO a,
                            INFORMATION_SCHEMA.CLUSTER_CONFIG b
                       where a.type = 'tidb'
                         and a.SYSTEM_TYPE = 'system'
                         and a.SYSTEM_NAME = 'sysctl'
                         and a.name = 'kernel.hostname'
                         and a.INSTANCE = b.INSTANCE
                         and b.`key` = 'status.status-port') b on a.INSTANCE = b.new_instance;
    """)
    for row in cursor:
        connection_info = ConnectionInfo()
        connection_info.type = row[0]
        connection_info.hostname = row[1]
        connection_info.instance = row[2]
        connection_info.connection_count = row[3]
        connection_info.active_connection_count = row[4]
        connection_infos.append(connection_info)
    cursor.close()
    return connection_infos


# 高并发情况下活动连接数情况分析
# select
#     count(*) as total_active_sessions,
#     coalesce(sum(case when ctt.STATE = "LockWaiting" then 1 else 0 end),0) as lock_waiting_sessions
# from INFORMATION_SCHEMA.CLUSTER_PROCESSLIST cp
#          join INFORMATION_SCHEMA.CLUSTER_TIDB_TRX ctt on cp.INSTANCE = ctt.INSTANCE and cp.ID = ctt.SESSION_ID
# where cp.COMMAND != 'Sleep' and cp.ID != CONNECTION_ID();
class ActiveSessionCount(BaseTable):
    def __init__(self):
        self.total_active_sessions = 0
        self.lock_waiting_sessions = 0
        self.metadata_lock_waiting_sessions = 0
        super().__init__()

def get_active_session_count(conn):
    """
    获取数据库中所有节点的活动连接数信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[ActiveSessionCount]
    """
    active_session_counts: List[ActiveSessionCount] = []
    cursor = conn.cursor()
    cursor.execute("""
    select /*+ MAX_EXECUTION_TIME(10000) MEMORY_QUOTA(1024 MB) */
        count(*) as total_active_sessions,
        coalesce(sum(case when ctt.STATE = "LockWaiting" then 1 else 0 end),0) as lock_waiting_sessions,
        (select count(distinct session_id) from mysql.tidb_mdl_view tmv) as metadata_lock_waiting_sessions
    from INFORMATION_SCHEMA.CLUSTER_PROCESSLIST cp
             join INFORMATION_SCHEMA.CLUSTER_TIDB_TRX ctt on cp.INSTANCE = ctt.INSTANCE and cp.ID = ctt.SESSION_ID
    where cp.COMMAND != 'Sleep' and cp.ID != CONNECTION_ID();
    """)
    for row in cursor:
        active_session_count = ActiveSessionCount()
        active_session_count.total_active_sessions = row[0]
        active_session_count.lock_waiting_sessions = row[1]
        active_session_count.metadata_lock_waiting_sessions = row[2]
        active_session_counts.append(active_session_count)
    cursor.close()
    return active_session_counts

# 查看元数据锁等待情况，到数据库中执行时候可能会偏慢
# select tmv.job_id                                        as ddl_job,
#        concat('admin cancel ddl jobs ', tmv.job_id, ';') as cancel_ddl_job,
#        tmv.db_name                                          ddl_job_dbname,
#        tmv.table_name                                       ddl_job_tablename,
#        tmv.query                                            ddl_sql,
#        tmv.session_id                                    as waitter_session_id,
#        tmv.txnstart                                      as waitter_txnstart,
#        sql_digests                                       as waitter_sqls
# from mysql.tidb_mdl_view tmv;
class MetadataLockWait(BaseTable):
    def __init__(self):
        self.ddl_job = ""
        self.cancel_ddl_job = ""
        self.ddl_job_dbname = ""
        self.ddl_job_tablename = ""
        self.ddl_sql = ""
        self.waitter_session_id = 0
        self.waitter_sqls = ""
        super().__init__()

def get_metadata_lock_wait(conn):
    """
    获取数据库中所有节点的元数据锁等待情况
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[MetadataLockWait]
    """
    sql_text = """
    select /*+ MAX_EXECUTION_TIME(10000) MEMORY_QUOTA(1024 MB) */
           tmv.job_id                                        as ddl_job,
           concat('admin cancel ddl jobs ', tmv.job_id, ';') as cancel_ddl_job,
           tmv.db_name                                          ddl_job_dbname,
           tmv.table_name                                       ddl_job_tablename,
           tmv.query                                            ddl_sql,
           tmv.session_id                                    as waitter_session_id,
           sql_digests                                       as waitter_sqls
    from mysql.tidb_mdl_view tmv;
    """
    metadata_lock_waits: List[MetadataLockWait] = []
    cursor = conn.cursor()
    try:
        cursor.execute(sql_text)
    except Exception as e:
        logging.error(f"Execute sql failed: {e}, {traceback.format_exc()}")
        return metadata_lock_waits
    for row in cursor:
        metadata_lock_wait = MetadataLockWait()
        metadata_lock_wait.ddl_job = row[0]
        metadata_lock_wait.cancel_ddl_job = row[1]
        metadata_lock_wait.ddl_job_dbname = row[2]
        metadata_lock_wait.ddl_job_tablename = row[3]
        metadata_lock_wait.ddl_sql = row[4]
        metadata_lock_wait.waitter_session_id = row[5]
        metadata_lock_wait.waitter_sqls = row[6]
        metadata_lock_waits.append(metadata_lock_wait)
    cursor.close()
    return metadata_lock_waits


# 查找锁源头
# WITH RECURSIVE lock_chain AS (
#     -- 初始查询：获取锁等待链，并为锁源头设置级别为 0
#     SELECT dlw.trx_id                 AS waiting_trx_id,
#            dlw.current_holding_trx_id AS holding_trx_id,
#            0                          AS level
#     FROM information_schema.data_lock_waits dlw
#     WHERE dlw.current_holding_trx_id is not null and dlw.current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
#     UNION ALL
#     -- 递归查询：根据持锁事务查找其上的等待链
#     SELECT lc.waiting_trx_id,
#            dlw.current_holding_trx_id,
#            lc.level + 1 AS level
#     FROM lock_chain lc
#              JOIN
#          information_schema.data_lock_waits dlw
#          ON
#              lc.holding_trx_id = dlw.trx_id)
# -- 查询最终结果，关联详细信息
# SELECT
#     -- 关联等待事务的信息
#     pl.instance                                                      AS waiting_instance,
#     pl.user                                                          AS waiting_user,
#     SUBSTRING_INDEX(pl.host, ':', 1)                                 AS waiting_client_ip,
#     lc.waiting_trx_id                                                AS waiting_transaction,
#     TIMESTAMPDIFF(SECOND, wt.WAITING_START_TIME, NOW())                      AS waiting_duration_sec,
#     wt.CURRENT_SQL_DIGEST                                            AS waiting_current_sql_digest,
#     LEFT(wt.CURRENT_SQL_DIGEST_TEXT, 100)                            AS waiting_sql,
#     -- 关联持锁事务的信息
#     case when lc.level = 0 then '锁等待链源头->' else '中间节点->' end as lock_chain_node_type,
#     ht.session_id                                                    AS holding_session_id,
#     -- 删除语句需要去重
#     CONCAT('kill tidb ', ht.session_id, ';')                        AS kill_holding_session_cmd,
#     pl_hold.instance                                                 AS holding_instance,
#     pl_hold.user                                                     AS holding_user,
#     SUBSTRING_INDEX(pl_hold.host, ':', 1)                            AS holding_client_ip,
#     lc.holding_trx_id                                                AS holding_transaction,
#     -- 持锁事务的 SQL Digest
#     COALESCE(
#             ht.current_sql_digest,
#             JSON_EXTRACT(ht.all_sql_digests, '$[0]')
#     )                                                                AS holding_sql_digest,
#     -- 持锁事务的 SQL 来源
#     CASE
#         WHEN ht.current_sql_digest IS NOT NULL THEN 'CURRENT'
#         ELSE 'LAST'
#         END                                                          AS holding_sql_source,
#     -- 持锁事务的 SQL 语句
#     CASE
#         WHEN ht.current_sql_digest IS NOT NULL THEN LEFT(ht.current_sql_digest_text, 100)
#         -- 如果只打印一条记录，很多情况下为NULL，因此需要打印所有SQL，但是每条语句最多打印100个字符
#         ELSE TIDB_DECODE_SQL_DIGESTS(ht.all_sql_digests, 100)
#         END                                                          AS holding_sql
# FROM lock_chain lc
# -- 关联等待事务的详细信息
#          LEFT JOIN
#      information_schema.cluster_tidb_trx wt
#      ON
#          lc.waiting_trx_id = wt.id
# -- 关联等待事务的会话和实例信息
#          LEFT JOIN
#      information_schema.cluster_processlist pl
#      ON
#          wt.instance = pl.instance AND wt.session_id = pl.id
# -- 关联持锁事务的详细信息
#          LEFT JOIN
#      information_schema.cluster_tidb_trx ht
#      ON
#          lc.holding_trx_id = ht.id
# -- 关联持锁事务的会话和实例信息
#          LEFT JOIN
#      information_schema.cluster_processlist pl_hold
#      ON
#          ht.instance = pl_hold.instance AND ht.session_id = pl_hold.id;
class LockChain(BaseTable):
    def __init__(self):
        self.waiting_instance = ""
        self.waiting_user = ""
        self.waiting_client_ip = ""
        self.waiting_transaction = ""
        self.waiting_duration_sec = 0
        self.waiting_current_sql_digest = ""
        self.waiting_sql = ""
        self.lock_chain_node_type = ""
        self.holding_session_id = 0
        self.kill_holding_session_cmd = ""
        self.holding_instance = ""
        self.holding_user = ""
        self.holding_client_ip = ""
        self.holding_transaction = ""
        self.holding_sql_digest = ""
        self.holding_sql_source = ""
        self.holding_sql = ""
        super().__init__()

def get_lock_chain(conn):
    """
    获取数据库中所有节点的锁等待链信息
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[LockChain]
    """
    lock_chains: List[LockChain] = []
    cursor = conn.cursor()
    cursor.execute("""
    WITH RECURSIVE lock_chain AS (
        -- 初始查询：获取锁等待链，并为锁源头设置级别为 0
        SELECT dlw.trx_id                 AS waiting_trx_id,
               dlw.current_holding_trx_id AS holding_trx_id,
               0                          AS level
        FROM information_schema.data_lock_waits dlw
        WHERE dlw.current_holding_trx_id is not null and dlw.current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
        UNION ALL
        -- 递归查询：根据持锁事务查找其上的等待链
        SELECT lc.waiting_trx_id,
               dlw.current_holding_trx_id,
               lc.level + 1 AS level
        FROM lock_chain lc
                 JOIN
             information_schema.data_lock_waits dlw
             ON
                 lc.holding_trx_id = dlw.trx_id)
    -- 查询最终结果，关联详细信息
    SELECT
        -- 关联等待事务的信息
        pl.instance                                                      AS waiting_instance,
        pl.user                                                          AS waiting_user,
        SUBSTRING_INDEX(pl.host, ':', 1)                                 AS waiting_client_ip,
        lc.waiting_trx_id                                                AS waiting_transaction,
        TIMESTAMPDIFF(SECOND, wt.WAITING_START_TIME, NOW())                      AS waiting_duration_sec,
        wt.CURRENT_SQL_DIGEST                                            AS waiting_current_sql_digest,
        LEFT(wt.CURRENT_SQL_DIGEST_TEXT, 100)                            AS waiting_sql,
        -- 关联持锁事务的信息
        case when lc.level = 0 then '锁等待链源头->' else '中间节点->' end as lock_chain_node_type,
        ht.session_id                                                    AS holding_session_id,
        -- 删除语句需要去重
        CONCAT('kill tidb ', ht.session_id, ';')                        AS kill_holding_session_cmd,
        pl_hold.instance                                                 AS holding_instance,
        pl_hold.user                                                     AS holding_user,
        SUBSTRING_INDEX(pl_hold.host, ':', 1)                            AS holding_client_ip,
        lc.holding_trx_id                                                AS holding_transaction,
        -- 持锁事务的 SQL Digest
        COALESCE(
                ht.current_sql_digest,
                JSON_EXTRACT(ht.all_sql_digests, '$[0]')
        )                                                                AS holding_sql_digest,
        -- 持锁事务的 SQL 来源
        CASE
            WHEN ht.current_sql_digest IS NOT NULL THEN 'CURRENT'
            ELSE 'LAST'
            END                                                          AS holding_sql_source,
        -- 持锁事务的 SQL 语句
        CASE
            WHEN ht.current_sql_digest IS NOT NULL THEN LEFT(ht.current_sql_digest_text, 100)
            -- 如果只打印一条记录，很多情况下为NULL，因此需要打印所有SQL，但是每条语句最多打印100个字符
            ELSE TIDB_DECODE_SQL_DIGESTS(ht.all_sql_digests, 100)
            END                                                          AS holding_sql
    FROM lock_chain lc
    -- 关联等待事务的详细信息
             LEFT JOIN
         information_schema.cluster_tidb_trx wt
         ON
             lc.waiting_trx_id = wt.id
    -- 关联等待事务的会话和实例信息
                LEFT JOIN
            information_schema.cluster_processlist pl
            ON
                wt.instance = pl.instance AND wt.session_id = pl.id
    -- 关联持锁事务的详细信息
                LEFT JOIN
            information_schema.cluster_tidb_trx ht
            ON
                lc.holding_trx_id = ht.id
    -- 关联持锁事务的会话和实例信息
                LEFT JOIN
            information_schema.cluster_processlist pl_hold
            ON
                ht.instance = pl_hold.instance AND ht.session_id = pl_hold.id;
    """)
    for row in cursor:
        lock_chain = LockChain()
        lock_chain.waiting_instance = row[0]
        lock_chain.waiting_user = row[1]
        lock_chain.waiting_client_ip = row[2]
        lock_chain.waiting_transaction = row[3]
        lock_chain.waiting_duration_sec = row[4]
        lock_chain.waiting_current_sql_digest = row[5]
        lock_chain.waiting_sql = row[6]
        lock_chain.lock_chain_node_type = row[7]
        lock_chain.holding_session_id = row[8]
        lock_chain.kill_holding_session_cmd = row[9]
        lock_chain.holding_instance = row[10]
        lock_chain.holding_user = row[11]
        lock_chain.holding_client_ip = row[12]
        lock_chain.holding_transaction = row[13]
        lock_chain.holding_sql_digest = row[14]
        lock_chain.holding_sql_source = row[15]
        lock_chain.holding_sql = row[16]
        lock_chains.append(lock_chain)
    cursor.close()
    return lock_chains

# 判断锁源头是否发生变化
# -- 创建临时表用于存储不同周期的锁源头的session_id和周期
# create temporary table if not exists lock_source_check(source_session_id bigint,cycle int);
# -- 第一次查询：周期1
# insert into lock_source_check(source_session_id,cycle)
# WITH RECURSIVE lock_chain AS (
#     -- 基础查询：从当前锁等待中获取直接锁关系，并将锁源头的级别标记为 0
#     SELECT trx_id AS waiting_trx_id,
#         current_holding_trx_id AS holding_trx_id,
#         0 AS level
#     FROM information_schema.data_lock_waits
#     WHERE current_holding_trx_id is not null and current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
#     UNION ALL
#     -- 递归查询：将上一层持锁事务作为下一层等待事务，级别递增
#     SELECT lc.waiting_trx_id,
#         dlw.current_holding_trx_id AS holding_trx_id,
#         lc.level + 1 AS level
#     FROM lock_chain lc
#         JOIN information_schema.data_lock_waits dlw ON lc.holding_trx_id = dlw.trx_id
# )
# SELECT distinct ctx.session_id,1
# FROM lock_chain lc
#     left join information_schema.cluster_tidb_trx ctx on lc.holding_trx_id = ctx.id
# where lc.level = 0;
#
# -- 休眠5秒钟
# select sleep(5);
# -- 第二次查询：周期2
# insert into lock_source_check(source_session_id,cycle)
# WITH RECURSIVE lock_chain AS (
#     -- 基础查询：从当前锁等待中获取直接锁关系，并将锁源头的级别标记为 0
#     SELECT trx_id AS waiting_trx_id,
#         current_holding_trx_id AS holding_trx_id,
#         0 AS level
#     FROM information_schema.data_lock_waits
#     WHERE current_holding_trx_id is not null and current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
#     UNION ALL
#     -- 递归查询：将上一层持锁事务作为下一层等待事务，级别递增
#     SELECT lc.waiting_trx_id,
#         dlw.current_holding_trx_id AS holding_trx_id,
#         lc.level + 1 AS level
#     FROM lock_chain lc
#         JOIN information_schema.data_lock_waits dlw ON lc.holding_trx_id = dlw.trx_id
# )
# SELECT distinct ctx.session_id,2
# FROM lock_chain lc
#     left join information_schema.cluster_tidb_trx ctx on lc.holding_trx_id = ctx.id
# where lc.level = 0;
#
# -- 休眠5秒钟
# select sleep(5);
# -- 第三次查询：周期3
# insert into lock_source_check(source_session_id,cycle)
# WITH RECURSIVE lock_chain AS (
#     -- 基础查询：从当前锁等待中获取直接锁关系，并将锁源头的级别标记为 0
#     SELECT trx_id AS waiting_trx_id,
#         current_holding_trx_id AS holding_trx_id,
#         0 AS level
#     FROM information_schema.data_lock_waits
#     WHERE current_holding_trx_id is not null and current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
#     UNION ALL
#     -- 递归查询：将上一层持锁事务作为下一层等待事务，级别递增
#     SELECT lc.waiting_trx_id,
#         dlw.current_holding_trx_id AS holding_trx_id,
#         lc.level + 1 AS level
#     FROM lock_chain lc
#         JOIN information_schema.data_lock_waits dlw ON lc.holding_trx_id = dlw.trx_id
# )
# SELECT distinct ctx.session_id,3
# FROM lock_chain lc
#     left join information_schema.cluster_tidb_trx ctx on lc.holding_trx_id = ctx.id
# where lc.level = 0;
#
# -- 查询并检查每个 source_session_id 是否在所有3个周期内都存在
# select '------ 输出结果如下：------';
# SELECT
#     source_session_id,
#     -- 出现的次数，比如cycle1表示在第一个周期内该session_source_id出现的次数（在不重复的情况下为1次）
#     SUM(CASE WHEN cycle = 1 THEN 1 ELSE 0 END) AS cycle1,
#     SUM(CASE WHEN cycle = 2 THEN 1 ELSE 0 END) AS cycle2,
#     SUM(CASE WHEN cycle = 3 THEN 1 ELSE 0 END) AS cycle3,
#     CASE
#     WHEN SUM(CASE WHEN cycle = 1 THEN 1 ELSE 0 END) > 0 AND
#          SUM(CASE WHEN cycle = 2 THEN 1 ELSE 0 END) > 0 AND
#          SUM(CASE WHEN cycle = 3 THEN 1 ELSE 0 END) > 0
#         THEN '锁源头不变'
#     ELSE '锁源头发生变化'
#     END                                    AS status
# FROM lock_source_check
# GROUP BY source_session_id
# ORDER BY source_session_id;
#
# select '------ 输出结束------';
# drop temporary table if exists lock_source_check;
class LockSourceChange(BaseTable):
    def __init__(self):
        self.source_session_id = 0
        self.cycle1 = 0
        self.cycle2 = 0
        self.cycle3 = 0
        self.status = ""
        super().__init__()


def get_lock_source_change(conn):
    """
    获取数据库中所有节点的锁源头变化信息,该函数会等待多次
    :param conn: 数据库连接
    :type conn: pymysql.connections.Connection
    :rtype: List[LockSourceChange]
    """
    lock_source_changes: List[LockSourceChange] = []
    cursor = conn.cursor()
    cursor.execute("""create temporary table if not exists lock_source_check(source_session_id bigint,cycle int);""")
    cursor.execute("""insert into lock_source_check(source_session_id,cycle) 
WITH RECURSIVE lock_chain AS (
    -- 基础查询：从当前锁等待中获取直接锁关系，并将锁源头的级别标记为 0
    SELECT trx_id AS waiting_trx_id,
        current_holding_trx_id AS holding_trx_id,
        0 AS level
    FROM information_schema.data_lock_waits
    WHERE current_holding_trx_id is not null and current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
    UNION ALL
    -- 递归查询：将上一层持锁事务作为下一层等待事务，级别递增
    SELECT lc.waiting_trx_id,
        dlw.current_holding_trx_id AS holding_trx_id,
        lc.level + 1 AS level
    FROM lock_chain lc
        JOIN information_schema.data_lock_waits dlw ON lc.holding_trx_id = dlw.trx_id
)
SELECT distinct ctx.session_id,1
FROM lock_chain lc
    left join information_schema.cluster_tidb_trx ctx on lc.holding_trx_id = ctx.id
where lc.level = 0;""")
    cursor.execute("""select sleep(5);""")
    cursor.execute("""insert into lock_source_check(source_session_id,cycle) 
WITH RECURSIVE lock_chain AS (
    -- 基础查询：从当前锁等待中获取直接锁关系，并将锁源头的级别标记为 0
    SELECT trx_id AS waiting_trx_id,
        current_holding_trx_id AS holding_trx_id,
        0 AS level
    FROM information_schema.data_lock_waits
    WHERE current_holding_trx_id is not null and current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
    UNION ALL
    -- 递归查询：将上一层持锁事务作为下一层等待事务，级别递增
    SELECT lc.waiting_trx_id,
        dlw.current_holding_trx_id AS holding_trx_id,
        lc.level + 1 AS level
    FROM lock_chain lc
        JOIN information_schema.data_lock_waits dlw ON lc.holding_trx_id = dlw.trx_id
)
SELECT distinct ctx.session_id,2
FROM lock_chain lc
    left join information_schema.cluster_tidb_trx ctx on lc.holding_trx_id = ctx.id
where lc.level = 0;""")
    cursor.execute("""select sleep(5);""")
    cursor.execute("""insert into lock_source_check(source_session_id,cycle) 
WITH RECURSIVE lock_chain AS (
    -- 基础查询：从当前锁等待中获取直接锁关系，并将锁源头的级别标记为 0
    SELECT trx_id AS waiting_trx_id,
        current_holding_trx_id AS holding_trx_id,
        0 AS level
    FROM information_schema.data_lock_waits
    WHERE current_holding_trx_id is not null and current_holding_trx_id not in (select distinct trx_id from information_schema.data_lock_waits)
    UNION ALL
    -- 递归查询：将上一层持锁事务作为下一层等待事务，级别递增
    SELECT lc.waiting_trx_id,
        dlw.current_holding_trx_id AS holding_trx_id,
        lc.level + 1 AS level
    FROM lock_chain lc
        JOIN information_schema.data_lock_waits dlw ON lc.holding_trx_id = dlw.trx_id
)
SELECT distinct ctx.session_id,3
FROM lock_chain lc
    left join information_schema.cluster_tidb_trx ctx on lc.holding_trx_id = ctx.id
where lc.level = 0;""")
    # 开始查询结果
    sql_text = """SELECT 
    source_session_id,
    -- 出现的次数，比如cycle1表示在第一个周期内该session_source_id出现的次数（在不重复的情况下为1次）
    SUM(CASE WHEN cycle = 1 THEN 1 ELSE 0 END) AS cycle1,
    SUM(CASE WHEN cycle = 2 THEN 1 ELSE 0 END) AS cycle2,
    SUM(CASE WHEN cycle = 3 THEN 1 ELSE 0 END) AS cycle3,
    CASE
    WHEN SUM(CASE WHEN cycle = 1 THEN 1 ELSE 0 END) > 0 AND
         SUM(CASE WHEN cycle = 2 THEN 1 ELSE 0 END) > 0 AND
         SUM(CASE WHEN cycle = 3 THEN 1 ELSE 0 END) > 0
        THEN '锁源头不变'
    ELSE '锁源头发生变化'
    END                                    AS status
FROM lock_source_check
GROUP BY source_session_id
ORDER BY source_session_id;"""
    cursor.execute(sql_text)
    for row in cursor:
        lock_source_change = LockSourceChange()
        lock_source_change.source_session_id = row[0]
        lock_source_change.cycle1 = row[1]
        lock_source_change.cycle2 = row[2]
        lock_source_change.cycle3 = row[3]
        lock_source_change.status = row[4]
        lock_source_changes.append(lock_source_change)
    cursor.execute("""drop temporary table if exists lock_source_check;""")
    cursor.close()
    return lock_source_changes

# 当前活动连接数信息
# -- tiflash中process_keys都为0，因为不走tikv。因此这里暂未兼容走tiflash存储引擎的语句。
# set session group_concat_max_len = 5242880;
# with processlist as (select a.*, b.info as current_sql_text
#                      from (select instance,
#                                   digest,
#                                   GROUP_CONCAT(DISTINCT CONCAT(USER, '(', user_count, ')') SEPARATOR
#                                                ', ')                                                AS user_access,
#                                   GROUP_CONCAT(DISTINCT CONCAT(ip_address, '(', ip_count, ')') SEPARATOR
#                                                ', ')                                                AS ip_access,
#                                   -- 将id 用group_concat聚合，因为一个digest可能对应多个id
#                                   group_concat(id)                                                  as id_list,
#                                   concat(group_concat(concat('kill tidb ', id) separator ';'), ';') as id_list_kill,
#                                   count(*)                                                          as active_count,
#                                   avg(time) * 2                                                     as active_avg_time,
#                                   sum(time)                                                         as active_total_time,
#                                   sum(mem)                                                          as active_total_mem,
#                                   sum(disk)                                                         as active_total_disk
#                            from (SELECT *,
#                                         SUBSTRING_INDEX(HOST, ':', 1)                                      AS ip_address,
#                                         COUNT(*) OVER (PARTITION BY DIGEST, USER)                          AS user_count,
#                                         COUNT(*) OVER (PARTITION BY DIGEST, SUBSTRING_INDEX(HOST, ':', 1)) AS ip_count
#                                  FROM INFORMATION_SCHEMA.CLUSTER_PROCESSLIST) as subquery
#                            where command != 'Sleep'
#                              and id != connection_id()
#                            group by instance, digest) a
#                               left join (select instance,
#                                                 digest,
#                                                 info,
#                                                 row_number() over (partition by instance,digest) as nbr
#                                          from information_schema.cluster_processlist) b -- 从该表中获取当前执行的sql文本
#                                         on a.instance = b.instance and a.digest = b.digest and b.nbr = 1),
#      statements_history as (select *
#                             from (select instance,
#                                          digest,
#                                          plan_digest,
#                                          exec_count,
#                                          exec_count / NULLIF(timestampdiff(second, first_seen, last_seen), 0)       as qps,
#                                          avg_latency / 1000000000                                                   as avg_latency,
#                                          avg_processed_keys,
#                                          avg_total_keys,
#                                          avg_result_rows,
#                                          query_sample_text,
#                                          first_seen,
#                                          last_seen,
#                                          AVG_REQUEST_UNIT_READ,
#                                          row_number() over (partition by instance,digest order by first_seen desc ) as nbr
#                                   from (select *
#                                         from information_schema.cluster_statements_summary
#                                         union all
#                                         select *
#                                         from information_schema.cluster_statements_summary_history) a) statements
#                             where nbr = 1),
#      result as (select pl.instance                                              as instance,
#                        pl.id_list                                               as session_id_list,
#                        pl.id_list_kill                                          as id_list_kill,
#                        pl.user_access,
#                        pl.ip_access,
#                        pl.digest                                                as digest,
#                        sh.plan_digest                                           as plan_digest,
#                        pl.active_count,
#                        pl.active_avg_time,
#                        pl.active_total_time,
#                        pl.active_total_mem,
#                        pl.active_total_disk,
#                        sh.exec_count,
#                        sh.qps,
#                        sh.avg_latency,
#                        sh.avg_processed_keys,
#                        sh.avg_total_keys,
#                        sh.avg_result_rows,
#                        sh.avg_processed_keys / NULLIF(sh.avg_result_rows, 0)    as avg_scan_keys_per_row,
#                        coalesce(pl.current_sql_text, sh.query_sample_text)      as query_sample_text,
#                        sh.first_seen,
#                        sh.last_seen,
#                        sh.AVG_REQUEST_UNIT_READ,
#                        -- 对于7.1以后版本可以用AVG_REQUEST_UNIT_READ来计算factor
#                        -- pl.active_count * sh.AVG_REQUEST_UNIT_READ          as active_total_factor
#                        -- 计算该语句的耗时因子，即执行次数*平均耗时*平均处理的key数，7.1以下版本没有AVG_REQUEST_UNIT_READ，所以用avg_processed_keys代替
#                        pl.active_count * sh.avg_latency * sh.avg_processed_keys as active_total_factor
#
#                 from processlist as pl
#                          left join statements_history as sh on pl.instance = sh.instance and pl.digest = sh.digest)
#
# select instance,                                                                             -- 实例名称
#        digest,                                                                               -- sql指纹
#        active_count,                                                                         -- 当前活动连接数
#        active_avg_time                                          as active_avg_time_s,        -- 预估平均每条语句执行时间（秒），从语句执行开始到当前时间*2
#        active_total_time                                        as active_total_time_s,      -- 当前正在执行的相同SQL指纹的总耗时（秒），从语句执行开始到当前时间
#        active_total_mem / 1024 / 1024                           as active_total_mem_mb,      -- 当前正在执行的相同SQL指纹的总内存消耗
#        active_total_disk / 1024 / 1024                          as active_total_disk_mb,     -- 当前正在执行的相同SQL指纹的总益处到磁盘消耗
#        -- todo 如果statement_history视图中没有该语句，那么可能对历史的分析不准确，导致找根因SQL时会有误差
#        exec_count,                                                                           -- 该语句在statement_history内存中保留的执行次数
#        qps,                                                                                  -- 该语句在整个库总平均每秒执行次数，计算first_seen和last_seen之间执行的次数
#        plan_digest,                                                                          -- 执行计划的指纹信息
#        avg_latency                                              as avg_latency_s,            -- 在statement_history中每条语句的平均执行时间，历史记录往往更真实
#        avg_processed_keys,                                                                   -- 平均每条语句扫描过的keys数量（gc时间之内所有版本扫描，需mvcc判断）
#        avg_total_keys,                                                                       -- 平均每条记录扫描过的keys数量（gc时间之外的已经插入墓碑标记但是未被rockdb清理的版本）
#        avg_result_rows,                                                                      -- 平均每条语句返回的行数
#        avg_scan_keys_per_row,                                                                -- 平均每行扫描的keys数量（包括表和索引）
#        query_sample_text, -- 该语句的样例文本（带有具体值）
#        substring(replace(query_sample_text, '\n', ' '), 1, 200) as query_sample_text_len200, -- 该语句的样例文本（带有具体值）
#        first_seen,                                                                           -- 该语句在statement_history中的首次出现时间
#        last_seen,                                                                            -- 该语句在statement_history中的最后一次出现时间
#        active_total_factor,                                                                  -- 该语句的耗时因子，改值越大表示该语句越耗时
#        active_total_factor_percent,                                                          -- 该语句的耗时因子在所有相同指纹的语句中的占比
#        case
#            -- 如果执行时间超过1秒，且该语句耗时因子占比超过1/count(*)，则认为是慢查询
#            when active_total_factor_percent >= 1 / NULLIF(count(*) over (), 0) and active_avg_time >= 1 then 'yes'
#            -- 如果没在statements_history表中找到该语句，且执行时间超过1秒，则认为是慢查询
#            when active_total_factor_percent is null and active_avg_time >= 1 then 'yes'
#            else 'no' end                                        as expensive_sql,            -- 是否慢查询
#        user_access,                                                                          -- 用户分布
#        ip_access,                                                                            -- 客户端IP分布
#        session_id_list,                                                                      -- 相同的sql指纹对应的session_id列表
#        id_list_kill                                                                         -- session_id列表的kill形式展现，方便复制到tidb控制台执行，结合set session group_concat_max_len = 5242880;使用，避免被截断
#
# from (select *,
#              round(100 * active_total_factor / NULLIF(sum(active_total_factor) over (), 0),
#                    2) as active_total_factor_percent
#       from result) as t;

class ActiveConnectionInfo(BaseTable):
    def __init__(self):
        self.instance = ""
        self.digest = ""
        self.active_count = 0
        self.active_avg_time_s = 0
        self.active_total_time_s = 0
        self.active_total_mem_mb = 0
        self.active_total_disk_mb = 0
        self.exec_count = 0
        self.qps = 0
        self.plan_digest = ""
        self.avg_latency_s = 0
        self.avg_processed_keys = 0
        self.avg_total_keys = 0
        self.avg_result_rows = 0
        self.avg_scan_keys_per_row = 0
        self.query_sample_text = ""
        self.query_sample_text_len200 = ""
        self.first_seen = datetime.now()
        self.last_seen = datetime.now()
        self.active_total_factor = 0
        self.active_total_factor_percent = 0
        self.expensive_sql = ""
        self.user_access = ""
        self.ip_access = ""
        self.session_id_list = ""
        self.id_list_kill = ""
        super().__init__()

def get_active_connection_info(conn):
    sql_text = """-- tiflash中process_keys都为0，因为不走tikv。因此这里暂未兼容走tiflash存储引擎的语句。
with processlist as (select a.*, b.info as current_sql_text
                     from (select instance,
                                  digest,
                                  GROUP_CONCAT(DISTINCT CONCAT(USER, '(', user_count, ')') SEPARATOR
                                               ', ')                                                AS user_access,
                                  GROUP_CONCAT(DISTINCT CONCAT(ip_address, '(', ip_count, ')') SEPARATOR
                                               ', ')                                                AS ip_access,
                                  -- 将id 用group_concat聚合，因为一个digest可能对应多个id
                                  group_concat(id)                                                  as id_list,
                                  concat(group_concat(concat('kill tidb ', id) separator ';'), ';') as id_list_kill,
                                  count(*)                                                          as active_count,
                                  avg(time) * 2                                                     as active_avg_time,
                                  sum(time)                                                         as active_total_time,
                                  sum(mem)                                                          as active_total_mem,
                                  sum(disk)                                                         as active_total_disk
                           from (SELECT *,
                                        SUBSTRING_INDEX(HOST, ':', 1)                                      AS ip_address,
                                        COUNT(*) OVER (PARTITION BY DIGEST, USER)                          AS user_count,
                                        COUNT(*) OVER (PARTITION BY DIGEST, SUBSTRING_INDEX(HOST, ':', 1)) AS ip_count
                                 FROM INFORMATION_SCHEMA.CLUSTER_PROCESSLIST) as subquery
                           where command != 'Sleep'
                             and id != connection_id()
                           group by instance, digest) a
                              left join (select instance,
                                                digest,
                                                info,
                                                row_number() over (partition by instance,digest) as nbr
                                         from information_schema.cluster_processlist) b -- 从该表中获取当前执行的sql文本
                                        on a.instance = b.instance and a.digest = b.digest and b.nbr = 1),
     statements_history as (select *
                            from (select instance,
                                         digest,
                                         plan_digest,
                                         exec_count,
                                         exec_count / NULLIF(timestampdiff(second, first_seen, last_seen), 0)       as qps,
                                         avg_latency / 1000000000                                                   as avg_latency,
                                         avg_processed_keys,
                                         avg_total_keys,
                                         avg_result_rows,
                                         query_sample_text,
                                         first_seen,
                                         last_seen,
                                         AVG_REQUEST_UNIT_READ,
                                         row_number() over (partition by instance,digest order by first_seen desc ) as nbr
                                  from (select *
                                        from information_schema.cluster_statements_summary
                                        union all
                                        select *
                                        from information_schema.cluster_statements_summary_history) a) statements
                            where nbr = 1),
     result as (select pl.instance                                              as instance,
                       pl.id_list                                               as session_id_list,
                       pl.id_list_kill                                          as id_list_kill,
                       pl.user_access,
                       pl.ip_access,
                       pl.digest                                                as digest,
                       sh.plan_digest                                           as plan_digest,
                       pl.active_count,
                       pl.active_avg_time,
                       pl.active_total_time,
                       pl.active_total_mem,
                       pl.active_total_disk,
                       sh.exec_count,
                       sh.qps,
                       sh.avg_latency,
                       sh.avg_processed_keys,
                       sh.avg_total_keys,
                       sh.avg_result_rows,
                       sh.avg_processed_keys / NULLIF(sh.avg_result_rows, 0)    as avg_scan_keys_per_row,
                       coalesce(pl.current_sql_text, sh.query_sample_text)      as query_sample_text,
                       sh.first_seen,
                       sh.last_seen,
                       sh.AVG_REQUEST_UNIT_READ,
                       -- 对于7.1以后版本可以用AVG_REQUEST_UNIT_READ来计算factor
                       -- pl.active_count * sh.AVG_REQUEST_UNIT_READ          as active_total_factor
                       -- 计算该语句的耗时因子，即执行次数*平均耗时*平均处理的key数，7.1以下版本没有AVG_REQUEST_UNIT_READ，所以用avg_processed_keys代替
                       pl.active_count * sh.avg_latency * sh.avg_processed_keys as active_total_factor

                from processlist as pl
                         left join statements_history as sh on pl.instance = sh.instance and pl.digest = sh.digest)

select instance,                                                                             -- 实例名称
       digest,                                                                               -- sql指纹
       active_count,                                                                         -- 当前活动连接数
       active_avg_time                                          as active_avg_time_s,        -- 预估平均每条语句执行时间（秒），从语句执行开始到当前时间*2
       active_total_time                                        as active_total_time_s,      -- 当前正在执行的相同SQL指纹的总耗时（秒），从语句执行开始到当前时间
       active_total_mem / 1024 / 1024                           as active_total_mem_mb,      -- 当前正在执行的相同SQL指纹的总内存消耗
       active_total_disk / 1024 / 1024                          as active_total_disk_mb,     -- 当前正在执行的相同SQL指纹的总益处到磁盘消耗
       -- todo 如果statement_history视图中没有该语句，那么可能对历史的分析不准确，导致找根因SQL时会有误差
       exec_count,                                                                           -- 该语句在statement_history内存中保留的执行次数
       qps,                                                                                  -- 该语句在整个库总平均每秒执行次数，计算first_seen和last_seen之间执行的次数
       plan_digest,                                                                          -- 执行计划的指纹信息
       avg_latency                                              as avg_latency_s,            -- 在statement_history中每条语句的平均执行时间，历史记录往往更真实
       avg_processed_keys,                                                                   -- 平均每条语句扫描过的keys数量（gc时间之内所有版本扫描，需mvcc判断）
       avg_total_keys,                                                                       -- 平均每条记录扫描过的keys数量（gc时间之外的已经插入墓碑标记但是未被rockdb清理的版本）
       avg_result_rows,                                                                      -- 平均每条语句返回的行数
       avg_scan_keys_per_row,                                                                -- 平均每行扫描的keys数量（包括表和索引）
       query_sample_text, -- 该语句的样例文本（带有具体值）
       substring(replace(query_sample_text, '\n', ' '), 1, 200) as query_sample_text_len200, -- 该语句的样例文本（带有具体值）
       first_seen,                                                                           -- 该语句在statement_history中的首次出现时间
       last_seen,                                                                            -- 该语句在statement_history中的最后一次出现时间
       active_total_factor,                                                                  -- 该语句的耗时因子，改值越大表示该语句越耗时
       active_total_factor_percent,                                                          -- 该语句的耗时因子在所有相同指纹的语句中的占比
       case
           -- 如果执行时间超过1秒，且该语句耗时因子占比超过1/count(*)，则认为是慢查询
           when active_total_factor_percent >= 1 / NULLIF(count(*) over (), 0) and active_avg_time >= 1 then 'yes'
           -- 如果没在statements_history表中找到该语句，且执行时间超过1秒，则认为是慢查询
           when active_total_factor_percent is null and active_avg_time >= 1 then 'yes'
           else 'no' end                                        as expensive_sql,            -- 是否慢查询
       user_access,                                                                          -- 用户分布
       ip_access,                                                                            -- 客户端IP分布
       session_id_list,                                                                      -- 相同的sql指纹对应的session_id列表
       id_list_kill                                                                         -- session_id列表的kill形式展现，方便复制到tidb控制台执行，结合set session group_concat_max_len = 5242880;使用，避免被截断

from (select *,
             round(100 * active_total_factor / NULLIF(sum(active_total_factor) over (), 0),
                   2) as active_total_factor_percent
      from result) as t"""
    active_connection_infos: List[ActiveConnectionInfo] = []
    cursor = conn.cursor()
    cursor.execute("set session group_concat_max_len = 5242880;")
    cursor.execute(sql_text)
    for row in cursor:
        active_connection_info = ActiveConnectionInfo()
        active_connection_info.instance = row[0]
        active_connection_info.digest = row[1]
        active_connection_info.active_count = row[2]
        active_connection_info.active_avg_time_s = row[3]
        active_connection_info.active_total_time_s = row[4]
        active_connection_info.active_total_mem_mb = row[5]
        active_connection_info.active_total_disk_mb = row[6]
        active_connection_info.exec_count = row[7]
        active_connection_info.qps = row[8]
        active_connection_info.plan_digest = row[9]
        active_connection_info.avg_latency_s = row[10]
        active_connection_info.avg_processed_keys = row[11]
        active_connection_info.avg_total_keys = row[12]
        active_connection_info.avg_result_rows = row[13]
        active_connection_info.avg_scan_keys_per_row = row[14]
        active_connection_info.query_sample_text = row[15]
        active_connection_info.query_sample_text_len200 = row[16]
        active_connection_info.first_seen = row[17]
        active_connection_info.last_seen = row[18]
        active_connection_info.active_total_factor = row[19]
        active_connection_info.active_total_factor_percent = row[20]
        active_connection_info.expensive_sql = row[21]
        active_connection_info.user_access = row[22]
        active_connection_info.ip_access = row[23]
        active_connection_info.session_id_list = row[24]
        active_connection_info.id_list_kill = row[25]
        active_connection_infos.append(active_connection_info)
    cursor.close()
    return active_connection_infos

# CPU使用率
# select b.time, a.hostname, a.ip, a.types, b.cpu_used_percent
# from (select group_concat(type)                as types,
#              substring_index(instance, ':', 1) as ip,
#              value                             as hostname
#       from INFORMATION_SCHEMA.cluster_systeminfo
#       where name = 'kernel.hostname'
#       group by ip,
#                hostname) a,
#      (select time,
#              substring_index(instance, ':', 1) as ip,
#              round((100 - value), 2)           as cpu_used_percent
#       from METRICS_SCHEMA.node_cpu_usage
#       where mode = 'idle'
#         and time = now()) b
# where a.ip = b.ip;
class CpuUsage(BaseTable):
    def __init__(self):
        self.time = datetime.now()
        self.hostname = ""
        self.ip = ""
        self.types = ""
        self.cpu_used_percent = 0.0
        super().__init__()

def get_cpu_usage(conn):
    sql_text = """select b.time, a.hostname, a.ip, a.types, b.cpu_used_percent
from (select group_concat(type)                as types,
             substring_index(instance, ':', 1) as ip,
             value                             as hostname
      from INFORMATION_SCHEMA.cluster_systeminfo
      where name = 'kernel.hostname'
      group by ip,
               hostname) a,
     (select time,
             substring_index(instance, ':', 1) as ip,
             round((100 - value), 2)           as cpu_used_percent
      from METRICS_SCHEMA.node_cpu_usage
      where mode = 'idle'
        and time = now()) b
where a.ip = b.ip;"""
    cpu_usages: List[CpuUsage] = []
    cursor = conn.cursor()
    cursor.execute(sql_text)
    for row in cursor:
        cpu_usage = CpuUsage()
        cpu_usage.time = row[0]
        cpu_usage.hostname = row[1]
        cpu_usage.ip = row[2]
        cpu_usage.types = row[3]
        cpu_usage.cpu_used_percent = row[4]
        cpu_usages.append(cpu_usage)
    cursor.close()
    return cpu_usages

# 查看IO响应时间，查看最近1个小时之内的情况
# with node_hostname_map as (select group_concat(type)                as types,
#                                   substring_index(instance, ':', 1) as ip,
#                                   value                             as hostname
#                            from INFORMATION_SCHEMA.cluster_systeminfo
#                            where name = 'kernel.hostname'
#                            group by ip, hostname),
#      device_disk_info as (select substring_index(instance, ':', 1) as ip,
#                                  device_name,
#                                  value
#                           from INFORMATION_SCHEMA.CLUSTER_HARDWARE
#                           where DEVICE_TYPE = 'disk'
#                             and name = 'path'
#                           group by ip, device_name, value),
#      device_mapping_info as (select a.ip,
#                                     a.device_name as dm_device,
#                                     a.value       as mount_point,
#                                     b.device_name as mapper_device
#                              from device_disk_info a
#                                       join device_disk_info b
#                                            on a.ip = b.ip and a.VALUE = b.VALUE and a.DEVICE_NAME like 'dm-%'
#                                                and b.DEVICE_NAME like '/dev/mapper/%'
#                              order by a.ip),
#      aggregated_iops as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                 instance,
#                                 device,
#                                 max(value)                          as value
#                          from METRICS_SCHEMA.node_disk_iops
#                          where time between now() - interval 60 minute and now()
#                          group by time_group, instance, device),
#      aggregated_data_with_host as (select a.time_group,
#                                           a.instance,
#                                           h.hostname,
#                                           h.types as types_on_host,
#                                           a.device,
#                                           a.value
#                                    from aggregated_iops a
#                                             left join node_hostname_map h on substring_index(a.instance, ':', 1) = h.ip),
#      aggregated_latency as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                    instance,
#                                    device,
#                                    max(value)                          as value
#                             from METRICS_SCHEMA.node_disk_read_latency
#                             where time between now() - interval 60 minute and now()
#                             group by time_group, instance, device),
#      aggregated_write_latency as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                          instance,
#                                          device,
#                                          max(value)                          as value
#                                   from METRICS_SCHEMA.node_disk_write_latency
#                                   where time between now() - interval 60 minute and now()
#                                   group by time_group, instance, device),
#      aggregated_io_util as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                    instance,
#                                    device,
#                                    max(value)                          as value
#                             from METRICS_SCHEMA.node_disk_io_util
#                             where time between now() - interval 60 minute and now()
#                             group by time_group, instance, device),
#      aggregated_read_bytes as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                       instance,
#                                       device,
#                                       max(value)                          as value
#                                from METRICS_SCHEMA.tikv_disk_read_bytes
#                                where time between now() - interval 60 minute and now()
#                                group by time_group, instance, device),
#      aggregated_write_bytes as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                        instance,
#                                        device,
#                                        max(value)                          as value
#                                 from METRICS_SCHEMA.tikv_disk_write_bytes
#                                 where time between now() - interval 60 minute and now()
#                                 group by time_group, instance, device),
#      aggregated_cpu_usage as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
#                                      instance,
#                                      max(value)                          as value,
#                                      mode
#                               from METRICS_SCHEMA.node_cpu_usage
#                               where time between now() - interval 60 minute and now()
#                               group by time_group, instance, mode)
#
# select /*+ MAX_EXECUTION_TIME(10000) MEMORY_QUOTA(1024 MB) */
#     a.time_group                                              as time,
#     a.instance,
#     a.hostname,
#     a.types_on_host,
#     a.device,
#     b.mapper_device,
#     b.mount_point,
#     round(a.value, 2)                                         as iops,
#     round(c.value, 2)                                         as io_util,
#     round((d.value + e.value) / 1024 / nullif(a.value, 0), 0) as io_size_kb,
#     round(f.value * 1000, 2)                                  as read_latency_ms,
#     round(g.value * 1000, 2)                                  as write_latency_ms,
#     round(d.value / 1024 / 1024, 2)                           as disk_read_bytes_mb,
#     round(e.value / 1024 / 1024, 2)                           as disk_write_bytes_mb,
#     round((100 - h.value) / 100, 2)                           as cpu_used
# from aggregated_data_with_host a
#          left join device_mapping_info b on substring_index(a.instance, ':', 1) = b.ip and a.device = b.dm_device
#          left join aggregated_io_util c
#                    on a.time_group = c.time_group and a.instance = c.instance and a.device = c.device
#          left join aggregated_read_bytes d
#                    on a.time_group = d.time_group and a.instance = d.instance and a.device = d.device
#          left join aggregated_write_bytes e
#                    on a.time_group = e.time_group and a.instance = e.instance and a.device = e.device
#          left join aggregated_latency f
#                    on a.time_group = f.time_group and a.instance = f.instance and a.device = f.device
#          left join aggregated_write_latency g
#                    on a.time_group = g.time_group and a.instance = g.instance and a.device = g.device
#          left join aggregated_cpu_usage h on a.instance = h.instance and a.time_group = h.time_group
# where h.mode = 'idle'
#   and a.device like 'dm-%'
#   and b.mount_point like '/%'
# order by a.instance, b.mount_point, a.time_group desc;

class IoResponseTime(BaseTable):
    def __init__(self):
        self.time = datetime.now()
        self.instance = ""
        self.hostname = ""
        self.types_on_host = ""
        self.device = ""
        self.mapper_device = ""
        self.mount_point = ""
        self.iops = 0.0
        self.io_util = 0.0
        self.io_size_kb = 0.0
        self.read_latency_ms = 0.0
        self.write_latency_ms = 0.0
        self.disk_read_bytes_mb = 0.0
        self.disk_write_bytes_mb = 0.0
        self.cpu_used = 0.0
        super().__init__()

def get_io_response_time(conn):
    sql_text = """with node_hostname_map as (select group_concat(type)                as types,
                                  substring_index(instance, ':', 1) as ip,
                                  value                             as hostname
                           from INFORMATION_SCHEMA.cluster_systeminfo
                           where name = 'kernel.hostname'
                           group by ip, hostname),
     device_disk_info as (select substring_index(instance, ':', 1) as ip,
                                 device_name,
                                 value
                          from INFORMATION_SCHEMA.CLUSTER_HARDWARE
                          where DEVICE_TYPE = 'disk'
                            and name = 'path'
                          group by ip, device_name, value),
     device_mapping_info as (select a.ip,
                                    a.device_name as dm_device,
                                    a.value       as mount_point,
                                    b.device_name as mapper_device
                             from device_disk_info a
                                      join device_disk_info b
                                           on a.ip = b.ip and a.VALUE = b.VALUE and a.DEVICE_NAME like 'dm-%'
                                               and b.DEVICE_NAME like '/dev/mapper/%'
                             order by a.ip),
     aggregated_iops as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                instance,
                                device,
                                max(value)                          as value
                         from METRICS_SCHEMA.node_disk_iops
                         where time between now() - interval 60 minute and now()
                         group by time_group, instance, device),
     aggregated_data_with_host as (select a.time_group,
                                          a.instance,
                                          h.hostname,
                                          h.types as types_on_host,
                                          a.device,
                                          a.value
                                   from aggregated_iops a
                                            left join node_hostname_map h on substring_index(a.instance, ':', 1) = h.ip),
     aggregated_latency as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                   instance,
                                   device,
                                   max(value)                          as value
                            from METRICS_SCHEMA.node_disk_read_latency
                            where time between now() - interval 60 minute and now()
                            group by time_group, instance, device),
     aggregated_write_latency as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                         instance,
                                         device,
                                         max(value)                          as value
                                  from METRICS_SCHEMA.node_disk_write_latency
                                  where time between now() - interval 60 minute and now()
                                  group by time_group, instance, device),
     aggregated_io_util as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                   instance,
                                   device,
                                   max(value)                          as value
                            from METRICS_SCHEMA.node_disk_io_util
                            where time between now() - interval 60 minute and now()
                            group by time_group, instance, device),
     aggregated_read_bytes as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                      instance,
                                      device,
                                      max(value)                          as value
                               from METRICS_SCHEMA.tikv_disk_read_bytes
                               where time between now() - interval 60 minute and now()
                               group by time_group, instance, device),
     aggregated_write_bytes as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                       instance,
                                       device,
                                       max(value)                          as value
                                from METRICS_SCHEMA.tikv_disk_write_bytes
                                where time between now() - interval 60 minute and now()
                                group by time_group, instance, device),
     aggregated_cpu_usage as (select date_format(time, '%Y-%m-%d %H:%i') as time_group,
                                     instance,
                                     max(value)                          as value,
                                     mode
                              from METRICS_SCHEMA.node_cpu_usage
                              where time between now() - interval 60 minute and now()
                              group by time_group, instance, mode)

select /*+ MAX_EXECUTION_TIME(10000) MEMORY_QUOTA(1024 MB) */
    a.time_group                                              as time,
    a.instance,
    a.hostname,
    a.types_on_host,
    a.device,
    b.mapper_device,
    b.mount_point,
    round(a.value, 2)                                         as iops,
    round(c.value, 2)                                         as io_util,
    round((d.value + e.value) / 1024 / nullif(a.value, 0), 0) as io_size_kb,
    round(f.value * 1000, 2)                                  as read_latency_ms,
    round(g.value * 1000, 2)                                  as write_latency_ms,
    round(d.value / 1024 / 1024, 2)                           as disk_read_bytes_mb,
    round(e.value / 1024 / 1024, 2)                           as disk_write_bytes_mb,
    round((100 - h.value) / 100, 2)                           as cpu_used
from aggregated_data_with_host a
         left join device_mapping_info b on substring_index(a.instance, ':', 1) = b.ip and a.device = b.dm_device
         left join aggregated_io_util c
                   on a.time_group = c.time_group and a.instance = c.instance and a.device = c.device
         left join aggregated_read_bytes d
                   on a.time_group = d.time_group and a.instance = d.instance and a.device = d.device
         left join aggregated_write_bytes e
                   on a.time_group = e.time_group and a.instance = e.instance and a.device = e.device
         left join aggregated_latency f
                   on a.time_group = f.time_group and a.instance = f.instance and a.device = f.device
         left join aggregated_write_latency g
                   on a.time_group = g.time_group and a.instance = g.instance and a.device = g.device
         left join aggregated_cpu_usage h on a.instance = h.instance and a.time_group = h.time_group
where h.mode = 'idle'
  and a.device like 'dm-%'
  and b.mount_point like '/%'
order by a.instance, b.mount_point, a.time_group desc;"""
    io_response_times: List[IoResponseTime] = []
    cursor = conn.cursor()
    cursor.execute(sql_text)
    for row in cursor:
        io_response_time = IoResponseTime()
        io_response_time.time = row[0]
        io_response_time.instance = row[1]
        io_response_time.hostname = row[2]
        io_response_time.types_on_host = row[3]
        io_response_time.device = row[4]
        io_response_time.mapper_device = row[5]
        io_response_time.mount_point = row[6]
        io_response_time.iops = row[7]
        io_response_time.io_util = row[8]
        io_response_time.io_size_kb = row[9]
        io_response_time.read_latency_ms = row[10]
        io_response_time.write_latency_ms = row[11]
        io_response_time.disk_read_bytes_mb = row[12]
        io_response_time.disk_write_bytes_mb = row[13]
        io_response_time.cpu_used = row[14]
        io_response_times.append(io_response_time)
    cursor.close()
    return io_response_times
# 查看数据库最近1小时的QPS情况
# select time,
#        round(sum(value), 2) as qps
# from METRICS_SCHEMA.tidb_qps
# where type in ('StmtSendLongData', 'Query', 'StmtExecute', 'StmtPrepare', 'StmtFetch')
#   and time between now() - interval 60 minute and now()
# group by time
# order by time;
class Qps(BaseTable):
    def __init__(self):
        self.time = datetime.now()
        self.qps = 0.0
        super().__init__()

def get_qps(conn):
    sql_text = """select time,
       round(sum(value), 2) as qps
from METRICS_SCHEMA.tidb_qps
where type in ('StmtSendLongData', 'Query', 'StmtExecute', 'StmtPrepare', 'StmtFetch')
  and time between now() - interval 60 minute and now()
group by time
order by time;"""
    qps: List[Qps] = []
    cursor = conn.cursor()
    cursor.execute(sql_text)
    for row in cursor:
        qps_info = Qps()
        qps_info.time = row[0]
        qps_info.qps = row[1]
        qps.append(qps_info)
    cursor.close()
    return qps

# 查看语句的平均响应时间
# select instance, time, round(1000 * avg(value), 2) as avg_response_time_ms
# from METRICS_SCHEMA.tidb_query_duration
# where quantile = 0.5
#   and time between now() - interval 60 minute and now()
# group by time, instance
# order by instance, time;
class AvgResponseTime(BaseTable):
    def __init__(self):
        self.instance = ""
        self.time = datetime.now()
        self.avg_response_time_ms = 0.0
        super().__init__()

def get_avg_response_time(conn):
    sql_text = """select instance, time, round(1000 * avg(value), 2) as avg_response_time_ms
from METRICS_SCHEMA.tidb_query_duration
where quantile = 0.5
  and time between now() - interval 60 minute and now()
group by time, instance
order by instance, time;"""
    avg_response_times: List[AvgResponseTime] = []
    cursor = conn.cursor()
    cursor.execute(sql_text)
    for row in cursor:
        avg_response_time = AvgResponseTime()
        avg_response_time.instance = row[0]
        avg_response_time.time = row[1]
        avg_response_time.avg_response_time_ms = row[2]
        avg_response_times.append(avg_response_time)
    cursor.close()
    return avg_response_times

# 查看连接数使用率情况
# select type,
#        hostname,
#        report_instance as instance,
#        conns           as connection_count,
#        max_conns       as configured_max_counnection_count,
#        conn_ratio      as connection_ratio
# from (select b.type,
#              b.hostname,
#              a.instance                                                                 as report_instance,
#              b.instance,
#              a.conns,
#              c.max_conns,
#              case when c.max_conns <= 0 then 0 else round(a.conns / c.max_conns, 2) end as conn_ratio
#       from (select instance, cast(value as signed) as conns
#             from METRICS_SCHEMA.tidb_connection_count
#             where time = NOW()) a
#                left join(select a.type,
#                                 a.instance,
#                                 a.value                                                   as hostname,
#                                 concat(substring_index(a.instance, ':', 1), ':', b.value) as new_instance
#                          from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO a,
#                               INFORMATION_SCHEMA.CLUSTER_CONFIG b
#                          where a.type = 'tidb'
#                            and a.SYSTEM_TYPE = 'system'
#                            and a.SYSTEM_NAME = 'sysctl'
#                            and a.name = 'kernel.hostname'
#                            and a.instance = b.INSTANCE
#                            and b.`key` = 'status.status-port') b on a.instance = b.new_instance
#                left join (select row_number() over (partition by instance) as nbr,
#                                  instance,
#                                  cast(value as signed)                     as max_conns
#                           from INFORMATION_SCHEMA.CLUSTER_CONFIG
#                           where `key` in ('max-server-connections', 'instance.max_connections')) c
#                          on b.INSTANCE = c.INSTANCE and c.nbr = 1) a;
class ConnectionUsage(BaseTable):
    def __init__(self):
        self.type = ""
        self.hostname = ""
        self.instance = ""
        self.connection_count = 0
        self.configured_max_counnection_count = 0
        self.connection_ratio = 0.0
        super().__init__()

def get_connection_usage(conn):
    sql_text = """select type,
       hostname,
       report_instance as instance,
       conns           as connection_count,
       max_conns       as configured_max_counnection_count,
       conn_ratio      as connection_ratio
from (select b.type,
             b.hostname,
             a.instance                                                                 as report_instance,
             b.instance,
             a.conns,
             c.max_conns,
             case when c.max_conns <= 0 then 0 else round(a.conns / c.max_conns, 2) end as conn_ratio
      from (select instance, cast(value as signed) as conns
            from METRICS_SCHEMA.tidb_connection_count
            where time = NOW()) a
               left join(select a.type,
                                a.instance,
                                a.value                                                   as hostname,
                                concat(substring_index(a.instance, ':', 1), ':', b.value) as new_instance
                         from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO a,
                              INFORMATION_SCHEMA.CLUSTER_CONFIG b
                         where a.type = 'tidb'
                           and a.SYSTEM_TYPE = 'system'
                           and a.SYSTEM_NAME = 'sysctl'
                           and a.name = 'kernel.hostname'
                           and a.instance = b.INSTANCE
                           and b.`key` = 'status.status-port') b on a.instance = b.new_instance
               left join (select row_number() over (partition by instance) as nbr,
                                 instance,
                                 cast(value as signed)                     as max_conns
                          from INFORMATION_SCHEMA.CLUSTER_CONFIG
                          where `key` in ('max-server-connections', 'instance.max_connections')) c
                         on b.INSTANCE = c.INSTANCE and c.nbr = 1) a;"""
    connection_usages: List[ConnectionUsage] = []
    cursor = conn.cursor()
    cursor.execute(sql_text)
    for row in cursor:
        connection_usage = ConnectionUsage()
        connection_usage.type = row[0]
        connection_usage.hostname = row[1]
        connection_usage.instance = row[2]
        connection_usage.connection_count = row[3]
        connection_usage.configured_max_counnection_count = row[4]
        connection_usage.connection_ratio = row[5]
        connection_usages.append(connection_usage)
    cursor.close()
    return connection_usages



if __name__ == "__main__":
    # 打印日志到终端，打印行号，日期等
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    set_max_memory()
    conn = pymysql.connect(host="192.168.31.201", port=4000, user="root", password="123", charset="utf8mb4",
                           database="information_schema",connect_timeout=10,
                           init_command="set session max_execution_time=30000")
    out_conn = sqlite3.connect("../dbinfo.db")
    out_conn.text_factory = str
    # out_conn = pymysql.connect(host="192.168.31.201", port=4000, user="root", password="123", charset="utf8mb4",database="test")
    SaveData(out_conn, get_variables, conn)
    SaveData(out_conn, get_column_collations, conn)
    SaveData(out_conn, get_user_privileges, conn)
    SaveData(out_conn, get_node_versions, conn)
    SaveData(out_conn, get_slow_query_info, conn, datetime.now() - timedelta(days=10), datetime.now())  # 默认查询最近一天的慢查询
    SaveData(out_conn,get_statement_history, conn)
    SaveData(out_conn, get_duplicate_indexes, conn)
    conn.close()
    out_conn.close()

    """variables = get_variables(conn)
    for variable in variables:
        print(f"{variable.type} {variable.name} {variable.value}")
    collations = get_column_collations(conn)
    for collation in collations:
        print(f"{collation.table_schema} {collation.table_name} {collation.column_name} {collation.collation_name}")
    privileges = get_user_privileges(conn)
    for privilege in privileges:
        print(f"{privilege.user}@{privilege.host} {','.join(privilege.privilege)}")
    versions = get_node_versions(conn)
    for version in versions:
        print(f"{version.node_type} {version.version} {version.git_hash}")
    start_time = datetime.strptime("2024-08-01 20:40:08", "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime("2024-08-10 19:40:08", "%Y-%m-%d %H:%M:%S")
    slow_queries = get_slow_query_info(conn, start_time, end_time)
    for slow_query in slow_queries:
        print(
            f"{slow_query.digest} {slow_query.plan_digest} {slow_query.query} {slow_query.plan} {slow_query.exec_count} {slow_query.succ_count} {slow_query.sum_query_time} {slow_query.avg_query_time} {slow_query.sum_total_keys} {slow_query.avg_total_keys} {slow_query.sum_process_keys} {slow_query.avg_process_keys} {slow_query.min_time} {slow_query.max_time} {slow_query.mem_max} {slow_query.disk_max} {slow_query.avg_result_rows} {slow_query.max_result_rows} {slow_query.plan_from_binding}")
    duplicate_indexes = get_duplicate_indexes(conn)
    for duplicate_index in duplicate_indexes:
        print(
            f"{duplicate_index.table_schema} {duplicate_index.table_name} {duplicate_index.index_name} {','.join(duplicate_index.columns)}")
        print(duplicate_index.create_table_sql())
        print(duplicate_index.insert_sql())
    conn.close()"""
