import logging
from datetime import datetime, timedelta
import pymysql
from typing import List
import traceback
from .utils import set_max_memory
import sqlite3
from .duplicate_index import Index, get_tableindexes, CONST_DUPLICATE_INDEX, CONST_SUSPECTED_DUPLICATE_INDEX

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
        self.min_time = ""
        self.max_time = ""
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
    SELECT ss.Digest,         -- SQL Digest
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
    cursor.execute(slow_query_sql)
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
        slow_query.min_time = row["min_time"]
        slow_query.max_time = row["max_time"]
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

    select EXEC_COUNT,STMT_TYPE,round(AVG_LATENCY/1000000000,3) as AVG_LATENCY,INSTANCE,SUMMARY_BEGIN_TIME,SUMMARY_END_TIME,FIRST_SEEN,LAST_SEEN,DIGEST,PLAN_DIGEST,round(SUM_LATENCY/1000000000,3) as SUM_LATENCY,AVG_MEM,AVG_DISK,AVG_RESULT_ROWS,AVG_AFFECTED_ROWS,AVG_PROCESSED_KEYS,AVG_TOTAL_KEYS,AVG_ROCKSDB_DELETE_SKIPPED_COUNT,AVG_ROCKSDB_KEY_SKIPPED_COUNT,AVG_ROCKSDB_BLOCK_READ_COUNT,SCHEMA_NAME,TABLE_NAMES,INDEX_NAMES,DIGEST_TEXT,QUERY_SAMPLE_TEXT,PREV_SAMPLE_TEXT,PLAN
    from top_sql limit 100000 -- 控制最多返回10万条
    """
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(statement_history_sql)
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
