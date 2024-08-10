# Desc: 获取慢查询信息

from datetime import datetime


class SlowQuery:
    def __init__(self):
        self.digest = ""
        self.plan_digest = ""
        self.query = ""
        self.plan = ""
        self.exec_count = 0
        self.succ_count = 0
        self.sum_query_time = 0
        self.avg_query_time = 0
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


# 获取慢查询信息
def get_slow_query_info(conn, start_time, end_time):
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
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    if start_time >= end_time or not start_time or not end_time:
        # 查询最近一天的慢查询
        start_time_str = "adddate(now(),INTERVAL -1 DAY)"
        end_time_str = "now()"
    slow_queries = []
    # get from https://tidb.net/blog/90e27aa0
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
    cursor = conn.cursor()
    cursor.execute(slow_query_sql)
    for row in cursor:
        slow_query = SlowQuery()
        slow_query.digest = row[0]
        slow_query.plan_digest = row[1]
        slow_query.query = row[2]
        slow_query.plan = row[3]
        slow_query.exec_count = row[4]
        slow_query.succ_count = row[5]
        slow_query.sum_query_time = row[6]
        slow_query.avg_query_time = row[7]
        slow_query.sum_total_keys = row[8]
        slow_query.avg_total_keys = row[9]
        slow_query.sum_process_keys = row[10]
        slow_query.avg_process_keys = row[11]
        slow_query.min_time = row[12]
        slow_query.max_time = row[13]
        slow_query.mem_max = row[14]
        slow_query.disk_max = row[15]
        slow_query.avg_result_rows = row[16]
        slow_query.max_result_rows = row[17]
        slow_query.plan_from_binding = row[18]
        slow_queries.append(slow_query)
    cursor.close()
    return slow_queries
