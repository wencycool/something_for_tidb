import logging
from pkg.utils import set_max_memory
import pymysql
import sqlite3
import argparse
import getpass
import shutil
from pathlib import Path
import yaml
from pkg.dbinfo import get_node_versions, get_variables, get_column_collations, get_user_privileges, \
    get_slow_query_info, get_duplicate_indexes, SaveData
from datetime import datetime, timedelta
from pkg.report import report as report_html

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
    user = args.user
    ip = args.host
    port = args.port
    password = args.password
    slowquery_start_time = datetime.now() - parse_since(args.since)
    slowquery_end_time = datetime.now()
    if not password:
        password = getpass.getpass("请输入密码:")
    if args.output_dir == "output":
        Path("output").mkdir(exist_ok=True)
    logging.info(f"输出目录:{args.output_dir}")
    if ip and ip != "127.0.0.1":
        if not args.cluster:
            args.cluster = "default"
        conn = pymysql.connect(host=ip, port=port, user=user, password=password, charset="utf8mb4",
                               database="information_schema", connect_timeout=10,
                               init_command="set session max_execution_time=30000")
        out_conn = sqlite3.connect(f"{args.output_dir}/{args.cluster}.sqlite3")
        out_conn.text_factory = str
        SaveData(out_conn, get_variables, conn)
        SaveData(out_conn, get_column_collations, conn)
        SaveData(out_conn, get_user_privileges, conn)
        SaveData(out_conn, get_node_versions, conn)
        SaveData(out_conn, get_slow_query_info, conn, datetime.now() - timedelta(days=10),
                 datetime.now())  # 默认查询最近一天的慢查询
        SaveData(out_conn, get_duplicate_indexes, conn)
        conn.close()
        out_conn.close()
    else:
        cluster_infos = get_cluster_infos()
        if args.cluster and args.cluster != "default":
            cluster_infos = [cluster for cluster in cluster_infos if cluster.cluster_name in args.cluster.split(",")]
        for cluster_info in cluster_infos:
            logging.info(f"开始获取{cluster_info.cluster_name}信息，ip:{cluster_info.ip},port:{cluster_info.port}")
            try:
                conn = pymysql.connect(host=cluster_info.ip, port=cluster_info.port, user=user, password=password,
                                       charset="utf8mb4",
                                       database="information_schema", connect_timeout=10,
                                       init_command="set session max_execution_time=30000")
                out_conn = sqlite3.connect(f"{args.output_dir}/{cluster_info.cluster_name}.sqlite3")
                out_conn.text_factory = str
                SaveData(out_conn, get_variables, conn)
                SaveData(out_conn, get_column_collations, conn)
                SaveData(out_conn, get_user_privileges, conn)
                SaveData(out_conn, get_node_versions, conn)
                SaveData(out_conn, get_slow_query_info, conn, slowquery_start_time, slowquery_end_time)
                SaveData(out_conn, get_duplicate_indexes, conn)
                conn.close()
                out_conn.close()
            except Exception as e:
                logging.error(f"获取{cluster_info.cluster_name}信息失败:{e}")
                continue


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
    report_parser = subparsers.add_parser("report", help="从sqlite3中获取信息生成html报表")
    report_parser.add_argument("-i","--db", type=str, help="sqlite3文件路径，如果是目录则会查找目录下的所有sqlite3文件")
    report_parser.add_argument("-o", "--output", type=str, help="输出html文件路径,默认当前路径", default=".")
    args = parser.parse_args()
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
