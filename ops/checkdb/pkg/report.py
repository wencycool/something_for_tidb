
import sqlite3
from .report_base import header, footer, generate_html_table, generate_html_chart,generate_html_chart_with_instance_and_mount

def fetch_data(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except sqlite3.OperationalError as e:
        return [], []
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    cursor.close()
    return column_names, rows

def report_queries():
    """
    打印的查询列表
    :return:
    """
    queries = {}
    queries["节点信息"] = [
        "table",
        "SELECT * FROM tidb_nodeinfo",
        "查询每个节点的信息，包括节点的IP地址、端口、状态、版本、启动时间"
    ]
    queries["CPU信息"] = [
        "table",
        "select a.*,b.cpu_used_percent from tidb_osinfo a,tidb_cpuusage b where a.hostname=b.hostname",
        "查询每个节点的CPU信息，包括CPU核数、CPU使用率"
    ]
    queries["磁盘使用率"] = [
        "table",
        "select hostname,ip_address,types_count,fstype,mountpoint,used_percent * 100 as used_percent,aval_size_gb,total_size_gb from tidb_diskinfo where mountpoint like '/tidb%' or mountpoint in ('/','/home')",
        "查询每个节点的磁盘使用率，包括磁盘挂载点、磁盘使用率、磁盘大小，只显示挂载点为/tidb开头和根目录的磁盘"
    ]
    queries["连接数分布情况"] = [
        "table",
        "SELECT type,hostname,instance,connection_count,configured_max_counnection_count,connection_ratio * 100 as connection_percent FROM tidb_connectioninfo",
        "各tidb节点连接数分布情况"
    ]
    queries["活动连接数汇总"] = [
        "table",
        """select sum(active_count) as active_sessions,
       (select count(*) from tidb_lockchain) as lock_waiting_sessions,
       (select count(*) from tidb_metadatalockwait) as ddl_waiting_sessions
       from tidb_activeconnectioninfo;""",
        "总活动连接数信息，包括正在执行语句和锁等待的连接数，这里的ddl_waiting_sessions指示DDL等待其它事务提交情况（在tidb v6.5以及以后版本中DDL不会阻塞其它事务提交。注意：因活动连接数和锁等待不是同一个语句获取的，因此可能存在锁等待数大于活动连接数情况。"
    ]
    queries["活动连接数详情"] = [
        "table",
        """
        select instance,
       substr(digest,1,16) as short_digest,
       active_count,
       active_avg_time_s as avg_time_s,
       active_total_time_s as total_time_s,
       expensive_sql,
       active_total_factor_percent as factor_percent,
       avg_total_keys,
       avg_processed_keys,
       avg_result_rows,
       digest,
       user_access,
       ip_access,
       active_total_mem_mb,
       active_total_disk_mb,
       exec_count as history_exec_count,
       qps as history_qps,
       avg_latency_s as history_avg_latency_s,
       avg_scan_keys_per_row as history_avg_scan_keys_per_row,
       first_seen,
       last_seen,
       active_total_factor,
       query_sample_text_len200,
       query_sample_text,
       session_id_list,
       id_list_kill
from tidb_activeconnectioninfo;
        """,
        "查询每个节点的活动连接数，expensive_sql表示该语句被评估为重度资源消耗语句，factor_percent表示当前语句在所有语句中影响因子占比，越高说明越耗资源"
    ]
    queries["锁等待详情"] = [
        "table",
        """SELECT
    substr(waiting_current_sql_digest,1,16) as short_waiting_digest,
    waiting_user,
    waiting_duration_sec,
    holding_session_id,
    holding_user,
    kill_holding_session_cmd,
    waiting_instance,
    waiting_client_ip,
    waiting_transaction,
    waiting_current_sql_digest,
    waiting_sql,
    holding_instance,
    holding_client_ip,
    holding_transaction,
    holding_sql_digest,
    holding_sql_source,
    holding_sql
    FROM tidb_lockchain""",
        "查询锁等待详情，以处于等待状态的session为主键"
    ]
    queries["锁源头行为判断"] = [
        "table",
        """
select source_session_id,
       status,
       concat('kill tidb ', source_session_id, ';') as kill_source_cmd,
       cycle1,
       cycle2,
       cycle3
from tidb_locksourcechange""",
        "查询锁等待的源头是否反复变化"
    ]
    queries["元数据锁"] = [
        "table",
        """select * from tidb_metadatalockwait""",
        "查询元数据锁等待详情，当存在元数据锁等待时，并不影响业务，只是DDL会等待DML提交"
    ]
    queries["集群QPS"] = [
        "chart",
        "select * from tidb_qps;",
        "查询集群QPS"
    ]
    queries["数据库平均响应时间"] = [
        "chart",
        "select time,instance,avg_response_time_ms from tidb_avgresponsetime;",
        "数据库语句平均响应时间"
    ]
    queries["磁盘IO响应时间"] = [
        "chart",
        "select time,instance,mount_point,iops,read_latency_ms,write_latency_ms,cpu_used * 100 as cpu_used_percent from tidb_ioresponsetime;",
        "磁盘IO响应时间"
    ]
    queries["StatementHistory"] = [
        "table",
        """
select substr(digest,1,16) as short_digest,
       exec_count,
       avg_latency as avg_latency_s,
       sum_latency as sum_latency_s,
       avg_processed_keys,
       avg_total_keys,
       avg_affected_rows,
       avg_result_rows,
       stmt_type,
       summary_begin_time,
       summary_end_time,
       first_seen,
       last_seen,
       instance,
       avg_mem,
       avg_disk,
       avg_rocksdb_block_read_count,
       avg_rocksdb_key_skipped_count,
       avg_rocksdb_delete_skipped_count,
       digest,
       plan_digest,
       schema_name,
       table_names,
       index_names,
       digest_text as sql_text,
       query_sample_text,
       prev_sample_text,
       plan
from tidb_statementhistory order by exec_count desc;""",
        "查询tidb的历史sql语句，包括sql语句和执行时间，选择大于50ms且执行次数top30的语句，avg_latency为平均执行时间（秒）。假设某种 SQL 每分钟都出现，那statements_summary_history 会保存这种 SQL 最近 12 个小时的数据。但如果某种 SQL 只在每天 00:00 ~ 00:30 出现，则 statements_summary_history 中会保存这种 SQL 24 个时间段的数据，每个时间段的间隔都是 1 天，所以会有这种 SQL 最近 24 天的数据。"
    ]
    queries["最近慢查询语句"] = [
        "table",
        """select substr(digest, 1, 16)                     as short_digest,
               exec_count,
               avg_query_time,
               sum_query_time,
               sum_process_keys,
               avg_process_keys,
               strftime('%Y-%m-%d %H:%M:%S', first_seen) as first_seen,
               strftime('%Y-%m-%d %H:%M:%S', last_seen)  as last_seen,
               sum_total_keys,
               avg_total_keys,
               avg_result_rows,
               max_result_rows,
               mem_max,
               disk_max,
               digest,
               plan_digest,
               succ_count,
               plan_from_binding,
               query,
               plan
        from tidb_slowquery
        order by exec_count desc;""",
        "查询慢查询信息，包括慢查询的sql语句和执行时间，按照Digest和Plan_digest进行分组聚合"
    ]
    return queries

def report(in_file, out_file):
    """
    从sqlite3中获取信息生成html报表
    :param in_file: sqlite3文件路径
    :type in_file: str
    :param out_file: 输出html文件路径
    :type out_file: str
    """
    conn = sqlite3.connect(in_file)
    conn.text_factory = str  # Set character set to UTF-8
    """
    queries = {
        "节点信息": ["table","SELECT type as 类型 FROM tidb_nodeinfo", "查询每个节点的信息，包括节点的IP地址、端口、状态、版本、启动时间、上线时间、下线时间、节点类型、节点角色、节点状态、节点状态描述"],
        "Os Info": ["table","SELECT * FROM tidb_osinfo", "查询每个节点的操作系统信息，包括CPU数和内存大小"],
        "Disk Info": ["table","SELECT * FROM tidb_diskinfo where used_percent >0.4", "查询每个节点的磁盘信息，包括磁盘的挂载点、磁盘大小、磁盘使用率（大于70%）"],
        "当前活动连接数汇总": ["table","SELECT * FROM tidb_activesessioncount", "总活动连接数信息"],
        "当前活动连接数": ["table","SELECT * FROM tidb_activeconnectioninfo", "查询每个节点的活动连接数"],
        "Memory Info": ["chart","SELECT time,used_percent,used_percent+0.5 FROM tidb_memoryusagedetail", "查询每个节点的内存信息，包括内存大小、内存使用率"],
        "Variables": ["table","SELECT * FROM tidb_variable", "查询tidb的配置信息,包括集群变量和系统全局变量"],
        "Column Collations": ["table","SELECT * FROM tidb_columncollation", "查询表字段上的排序规则，如果不是utf8mb4_bin则会列出（可能会导致索引失效）"],
        "User Privileges": ["table","SELECT * FROM tidb_userprivilege", "查询用户权限信息，包括用户的权限和角色,多个权限则排序后按照逗号分隔"],
        "Slow Queries":  ["table","SELECT * FROM tidb_slowquery", "查询慢查询信息，包括慢查询的sql语句和执行时间，按照Digest和Plan_digest进行分组聚合"],
        "Statement History": ["table","select * from tidb_statementhistory", "查询tidb的历史sql语句，包括sql语句和执行时间，选择大于50ms且执行次数top30的语句，avg_latency为平均执行时间（秒）。假设某种 SQL 每分钟都出现，那 statements_summary_history 中会保存这种 SQL 最近 12 个小时的数据。但如果某种 SQL 只在每天 00:00 ~ 00:30 出现，则 statements_summary_history 中会保存这种 SQL 24 个时间段的数据，每个时间段的间隔都是 1 天，所以会有这种 SQL 最近 24 天的数据。"],
        "Duplicate Indexes": ["table","SELECT * FROM tidb_duplicateindex",  "查询表上的冗余索引，state为DUPLICATE_INDEX表示冗余索引（最左前缀覆盖），state为SUSPECTED_DUPLICATE_INDEX表示疑似冗余索引"],
    }
    """
    queries = report_queries()
    html_content = header()
    html_content += "<body>\n"
    html_content += "<div class='sidebar'>\n"
    html_content += "<h2>导航</h2>\n"
    for title in queries.keys():
        html_content += f"<a href='#{title.replace(' ', '_')}'>{title}</a>\n"
    html_content += "</div>\n"

    html_content += "<div class='table-container'>\n"
    for idx, (title, query_list) in enumerate(queries.items()):
        query = query_list[1]
        describe = query_list[2]
        column_names, rows = fetch_data(conn, query)
        table_id = f"table_{idx}"
        html_content += f"<h2 id='{title.replace(' ', '_')}'>{title}</h2>\n"
        html_content += f"<small style='color: black; font-size: small;'>{describe}</small><br></br>\n"
        if query_list[0] == "chart":
            # column_names第一列为时间，后面的列为数据
            if title == "磁盘IO响应时间":
                html_content += generate_html_chart_with_instance_and_mount(table_id, column_names, rows, title)
            elif title == "数据库平均响应时间":
                html_content += generate_html_chart(table_id, column_names, rows, title, 1)
            else:
                html_content += generate_html_chart(table_id,column_names, rows, title, 0)
        else:
            html_content += generate_html_table(table_id, column_names, rows)
    html_content += "</div>\n"
    html_content += "</body>\n"
    html_content += footer()

    with open(out_file, 'w', encoding='utf-8') as file:
        file.write(html_content)

    conn.close()


def main():
    report('../default.sqlite3', 'output.html')


if __name__ == "__main__":
    main()
