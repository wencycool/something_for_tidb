import logging

from pkg.utils import set_max_memory
import pymysql
import sqlite3
import argparse
import getpass
import shutil
from pathlib import Path
import yaml
from pkg.dbinfo import *
from datetime import datetime, timedelta
from pkg.report import report as report_html
from dbutils.pooled_db import PooledDB
from concurrent.futures import ThreadPoolExecutor

functions_to_save = [
    get_connection_info,
    # get_active_session_count,
    get_lock_chain,
    get_lock_source_change,
    get_metadata_lock_wait,
    get_active_connection_info,
    get_qps,
    get_avg_response_time,
    get_io_response_time,
    get_node_info,
    get_os_info,
    get_cpu_usage,
    get_disk_info,
    get_table_info,
    get_memory_detail,
    get_statement_history,
    get_slow_query_info,
]
"""
    get_variables,
    get_column_collations,
    get_user_privileges,
    get_node_versions,
    get_statement_history,
    get_duplicate_indexes,
    """

# 初始化sqlite3中的表
def init_sqlite3_db(conn):
    # 如果在这里不初始化表，那么会在数据插入时自动创建表，但是要注意如果没有数据插入，那么表也不会创建
    table = {}
    table['tidb_connectioninfo'] = 'CREATE TABLE if not exists tidb_connectioninfo (type varchar(512),hostname varchar(512),instance varchar(512),connection_count int,active_connection_count int,configured_max_counnection_count int,connection_ratio float)'
    table['tidb_activesessioncount'] = 'CREATE TABLE if not exists tidb_activesessioncount (total_active_sessions int,lock_waiting_sessions int,metadata_lock_waiting_sessions int)'
    table['tidb_qps'] = 'CREATE TABLE if not exists tidb_qps (time text,qps float)'
    table['tidb_avgresponsetime'] = 'CREATE TABLE if not exists tidb_avgresponsetime (instance varchar(512),time text,avg_response_time_ms float)'
    table['tidb_ioresponsetime'] = 'CREATE TABLE if not exists tidb_ioresponsetime (time text,instance varchar(512),hostname varchar(512),types_on_host varchar(512),device varchar(512),mapper_device varchar(512),mount_point varchar(512),iops float,io_util float,io_size_kb float,read_latency_ms float,write_latency_ms float,disk_read_bytes_mb float,disk_write_bytes_mb float,cpu_used float)'
    table['tidb_nodeinfo'] = 'CREATE TABLE if not exists tidb_nodeinfo (type varchar(512),instance varchar(512),status_address varchar(512),version varchar(512),start_time varchar(512),uptime varchar(512),server_id int)'
    table['tidb_osinfo'] = 'CREATE TABLE if not exists tidb_osinfo (hostname varchar(512),ip_address varchar(512),types_count varchar(512),cpu_arch varchar(512),cpu_cores int,memory_capacity_gb float)'
    table['tidb_cpuusage'] = 'CREATE TABLE if not exists tidb_cpuusage (time text,hostname varchar(512),ip varchar(512),types varchar(512),cpu_used_percent float)'
    table['tidb_diskinfo'] = 'CREATE TABLE if not exists tidb_diskinfo (time varchar(512),ip_address varchar(512),hostname varchar(512),types_count varchar(512),fstype varchar(512),mountpoint varchar(512),aval_size_gb float,total_size_gb float,used_percent float)'
    table['tidb_memoryusagedetail'] = 'CREATE TABLE if not exists tidb_memoryusagedetail (time varchar(512),ip_address varchar(512),hostname varchar(512),types_count varchar(512),used_percent float)'
    table['tidb_lockchain'] = 'CREATE TABLE if not exists tidb_lockchain (waiting_instance varchar(512),waiting_user varchar(512),waiting_client_ip varchar(512),waiting_transaction varchar(512),waiting_duration_sec int,waiting_current_sql_digest varchar(512),waiting_sql varchar(512),lock_chain_node_type varchar(512),holding_session_id int,kill_holding_session_cmd varchar(512),holding_instance varchar(512),holding_user varchar(512),holding_client_ip varchar(512),holding_transaction varchar(512),holding_sql_digest varchar(512),holding_sql_source varchar(512),holding_sql varchar(512))'
    table['tidb_locksourcechange'] = 'CREATE TABLE if not exists tidb_locksourcechange (source_session_id int,cycle1 int,cycle2 int,cycle3 int,status varchar(512))'
    table['tidb_metadatalockwait'] = 'CREATE TABLE if not exists tidb_metadatalockwait (holding_session_id int,holding_sqls varchar(1024),waiting_ddl_job int,cancel_ddl_job varchar(512),ddl_job_dbname varchar(512),ddl_job_tablename varchar(512),ddl_sql varchar(512),ddl_is_locksource varchar(512),ddl_blocking_count int)'
    for table_name, sql in table.items():
        conn.execute(sql)

def set_logger(log_level):
    """
    设置日志级别
    :param log_level: 日志级别
    :return:
    """
    log_level = log_level.upper()
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=getattr(logging, log_level),
                        format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")


class TiUPNotFoundError(Exception):
    def __init__(self, message):
        super().__init__(message)


class TiDBClusterInfo:
    def __init__(self):
        self.cluster_name = ""
        self.ip = ""
        self.port = 0


def get_cluster_infos():
    # 查看是否存在tiup命令
    tiup_path = shutil.which("tiup")
    if not tiup_path:
        raise TiUPNotFoundError("tiup not found.")
    # 获取集群信息
    cluster_infos = []
    cluster_base_dir = Path(tiup_path).parent.parent.joinpath("storage/cluster/clusters")
    if not cluster_base_dir.exists():
        raise FileNotFoundError("cluster directory not found")
    for cluster_dir in cluster_base_dir.iterdir():
        if cluster_dir.is_dir():
            cluster_info = TiDBClusterInfo()
            cluster_info.cluster_name = cluster_dir.name
            meta_file = cluster_dir.joinpath("meta.yaml")
            yaml_file = yaml.load(meta_file.read_text(encoding='utf-8'), Loader=yaml.FullLoader)
            cluster_info.ip = yaml_file["topology"]["tidb_servers"][0]["host"]
            cluster_info.port = yaml_file["topology"]["tidb_servers"][0]["port"]
            cluster_infos.append(cluster_info)
    return cluster_infos


def parse_since(since):
    """
    解析since参数
    :param since: 时间间隔，格式为1d,1h,1m，比如查询最近10分钟慢日志则：10m
    :type since: str
    :return: datetime.datetime
    """
    if since[-1] not in ["d", "h", "m"]:
        raise ValueError("Invalid time interval")
    if not since[:-1].isdigit():
        raise ValueError("Invalid time interval")
    if since[-1] == "d":
        return timedelta(days=int(since[:-1]))
    elif since[-1] == "h":
        return timedelta(hours=int(since[:-1]))
    elif since[-1] == "m":
        return timedelta(minutes=int(since[:-1]))


def collect(args):
    """
    从TiDB集群中获取信息并储存到sqlite3中
    :param args: 命令行参数
    :type args: argparse.Namespace
    """
    def create_connection_pool(host, port, user, password):
        return PooledDB(
            creator=pymysql,
            maxconnections=10,
            mincached=2,
            maxcached=5,
            blocking=True,
            host=host,
            port=port,
            user=user,
            password=password,
            database='information_schema',
            charset='utf8mb4',
            init_command="set session max_execution_time=30000"
        )

    def execute_tasks(out_conn, pool, functions_to_save):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            new_conns = []
            # 记录总的需要打印的函数个数和已经打印的函数个数
            logging.info(f"一共有{len(functions_to_save)}个任务需要执行")
            for i,func in enumerate(functions_to_save):
                logging.info(f"开始执行{func.__name__},剩余任务数:{len(functions_to_save)-i-1}，剩余异步执行任务数:{len(futures)}")
                if func == get_slow_query_info:
                    conn2 = pool.connection()
                    new_conns.append(conn2)
                    futures.append(executor.submit(SaveData, out_conn, func, conn2, datetime.now() - timedelta(days=10),
                                                   datetime.now()))
                elif func in [get_lock_source_change,get_metadata_lock_wait]:
                    conn2 = pool.connection()
                    new_conns.append(conn2)
                    futures.append(executor.submit(SaveData, out_conn, func, conn2))
                else:
                    # 其它场景串型处理
                    conn = pool.connection()
                    SaveData(out_conn, func, conn)
                    conn.close()
            for i,future in enumerate(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"任务执行时出错: {e}")
                logging.info(f"剩余异步执行任务数:{len(futures)-i-1}")
            for conn in new_conns:
                conn.close()

    def process_cluster(cluster_name, ip, port, user, password):
        logging.info(f"开始获取{cluster_name}信息，ip:{ip}, port:{port}")
        try:
            pool = create_connection_pool(ip, port, user, password)
            # conn = pool.connection()
            # todo 先设置check_same_thread=False允许并行写入，后续考虑SQLiteConnectionManager替代
            sqlite3_file = f"{args.output_dir}/{cluster_name}.sqlite3"
            # 如果存在先删除
            if Path(sqlite3_file).exists():
                Path(sqlite3_file).unlink()
            out_conn = sqlite3.connect(sqlite3_file,check_same_thread=False)
            out_conn.text_factory = str
            # 初始化数据表，为了让活动连接数，锁等待的汇总数据和明细数据对齐，会采用明细数据做汇总的方式计算汇总数据
            init_sqlite3_db(out_conn)
            execute_tasks(out_conn, pool, functions_to_save)
            # conn.close()
            out_conn.close()
            if args.with_report:
                logging.info(f"开始生成{cluster_name}报表")
                report_html(f"{args.output_dir}/{cluster_name}.sqlite3", f"{args.output_dir}/{cluster_name}.html")
        except Exception as e:
            logging.error(f"获取{cluster_name}信息失败: {e}")

    user = args.user
    ip = args.host
    port = args.port
    password = args.password or getpass.getpass("请输入密码:")

    Path(args.output_dir).mkdir(exist_ok=True)
    logging.info(f"输出目录: {args.output_dir}")

    if ip and ip != "127.0.0.1":
        args.cluster = args.cluster or "default"
        process_cluster(args.cluster, ip, port, user, password)
    else:
        for cluster_info in get_cluster_infos():
            if not args.cluster or cluster_info.cluster_name in args.cluster.split(","):
                process_cluster(cluster_info.cluster_name, cluster_info.ip, cluster_info.port, user, password)


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
        qps_instance = Qps()
        qps_instance.time = row[0]
        qps_instance.qps = row[1]
        qps.append(qps_instance)
    cursor.close()
    return qps

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


def report(args):
    """
    从sqlite3中获取信息生成html报表
    :param args: 命令行参数
    :type args: argparse.Namespace
    """
    in_file = Path(args.db)
    if not in_file.exists():
        raise FileNotFoundError(f"{in_file} not found")
    in_files = []
    if in_file.is_dir():
        for each_file in in_file.iterdir():
            # 用sqlite3 尝试打开文件，如果不是sqlite3文件则跳过
            try:
                conn = sqlite3.connect(f'file:{each_file}?mode=ro', uri=True)
                conn.execute("select count(*) from sqlite_master limit 1")
                conn.close()
                in_files.append(each_file)
            except sqlite3.DatabaseError:
                logging.warning(f"{each_file}不是sqlite3文件，跳过")
                continue
    else:
        in_files.append(in_file)
    logging.info("共需要解析文件数:%d", len(in_files))
    for in_file in in_files:
        out_file = Path(args.output).joinpath(in_file.stem).with_suffix(".html")
        logging.info(f"开始解析{in_file}，输出文件:{out_file}")
        report_html(str(in_file), str(out_file))

def main():
    """
    支持从单个TiDB中获取信息并储存到sqlite3中
    支持从多个TiDB中获取信息并储存到sqlite3中，每一个集群一个文件
    可解析sqlite3，从中获取信息生成html报表
    :return:
    """
    parser = argparse.ArgumentParser(description="Check TiDB cluster info", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--log", type=str, default="INFO", help="打印日志级别")
    subparsers = parser.add_subparsers(dest="command", help="子命令帮助信息")
    collect_parser = subparsers.add_parser("collect", help="从TiDB集群中获取信息并储存到sqlite3中")
    collect_parser.add_argument("--cluster", type=str, help="集群名称,如果填写了ip地址信息则忽略这里的选择，这里主要用于在tiup上获取集群信息，集群名以逗号分隔，如果为空则会查找所有集群", default="default")
    collect_parser.add_argument("--host", type=str, help="集群ip地址",default="127.0.0.1")
    collect_parser.add_argument("--port", type=int, help="集群端口", default=4000)
    collect_parser.add_argument("--user", type=str, help="集群用户名", default="root")
    collect_parser.add_argument("--password", type=str, help="集群密码")
    collect_parser.add_argument("-o", "--output-dir", type=str, help="输出sqlite3文件路径,如果是多个集群则会在这个目录下生成多个文件，以集群名称命名", default="output")
    collect_parser.add_argument("--since", type=str, help="慢查询开始时间,格式为1d,1h,1m，比如查询最近10分钟慢日志则：--since=10m", default="1d")
    collect_parser.add_argument("--with-report", action="store_true", help="是否同时生成html报表")
    report_parser = subparsers.add_parser("report", help="从sqlite3中获取信息生成html报表")
    report_parser.add_argument("-i","--db", type=str, help="sqlite3文件路径，如果是目录则会查找目录下的所有sqlite3文件")
    report_parser.add_argument("-o", "--output", type=str, help="输出html文件路径,默认当前路径", default=".")
    args = parser.parse_args()
    # todo 打开内存控制参数
    set_max_memory()
    set_logger(args.log)
    if args.command == "collect":
        collect(args)
    elif args.command == "report":
        report(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
