#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# coding: utf-8

"""
存放常用的公共函数，方便调用
"""

import sys, threading, subprocess, tempfile
import re
import ast

# 判断python的版本
if sys.version_info < (3, 6):
    raise "python version need larger than 3.6"


def command_run(command, use_temp=False, timeout=30) -> (str, int):
    """

    :param str command: shell命令
    :param bool use_temp: 是否使用临时文件存储结果集，对于大结果集处理有效
    :param int timeout: 函数执行超时时间
    :return: 结果集和code
    """

    def _str(input):
        if isinstance(input, bytes):
            return str(input, 'UTF-8')
        return str(input)

    mutable = ['', '', None]
    # 用临时文件存放结果集效率太低，在tiup exec获取sstfile的时候因为数据量较大避免卡死建议开启，如果在获取tikv region property时候建议采用PIPE方式，效率更高
    if use_temp:
        out_temp = None
        out_fileno = None
        out_temp = tempfile.SpooledTemporaryFile(buffering=100 * 1024)
        out_fileno = out_temp.fileno()

        def target():
            mutable[2] = subprocess.Popen(command, stdout=out_fileno, stderr=out_fileno, shell=True)
            mutable[2].wait()

        th = threading.Thread(target=target)
        th.start()
        th.join(timeout)
        # 超时处理
        if th.is_alive():
            mutable[2].terminate()
            th.join()
            if mutable[2].returncode == 0:
                mutable[2].returncode = 9
            result = "Timeout Error!"
        else:
            out_temp.seek(0)
            result = out_temp.read()
        out_temp.close()
        return _str(result), mutable[2].returncode
    else:
        def target():
            mutable[2] = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            mutable[0], mutable[1] = mutable[2].communicate()

        th = threading.Thread(target=target)
        th.start()
        th.join(timeout)
        if th.is_alive():
            mutable[2].terminate()
            th.join()
            if mutable[2].returncode == 0:
                mutable[2].returncode = 1
        return _str(mutable[0]) + _str(mutable[1]), mutable[2].returncode


def check_number(s):
    """
    判断当前字符串是否数字类型，并返回浮点数
    :param s:
    :return: (数字,是否数字)
    """
    if re.match(r'^-?\d+$', s):
        return int(s), True
    elif re.match(r'^-?\d+\.\d+$', s):
        return float(s), True
    elif re.match(r'^-\d+$', s):
        return int(s), True
    else:
        return None, False


def check_list(s):
    """
    检查当前字符串是否列表形式，并返回列表
    :param s:
    :return: （列表，是否列表）
    """
    try:
        result = ast.literal_eval(s)
        if isinstance(result, list):
            return result, True
    except (SyntaxError, ValueError):
        return None, False
    return None, False


class Cluster:
    def __init__(self, cluster_name, user, version, path, private_key):
        self.cluster_name = cluster_name
        self.user = user
        self.version = version
        self.path = path
        self.private_key = private_key


def tiup_ok():
    """
    判断tiup命令是否正常
    :return:
    """
    cmd = "command -v tiup"
    _, recode = command_run(cmd)
    if recode != 0:
        return False
    return True


def list_clusters() -> [Cluster]:
    """
    列出当前tiup下所有集群
    :return:返回当前tiup下所有集群列表
    """
    clusters = []
    if not tiup_ok():
        raise Exception("tiup command cannot found")
    cmd = "tiup cluster list 2>/dev/null"
    result: str
    result, recode = command_run(cmd)
    if recode != 0:
        raise Exception(f"list cluster error,msg:{result},code:{recode}")
    can_print = False
    for each_line in result.split("\n"):
        if each_line.startswith("----"):
            can_print = True
            continue
        elif each_line.strip() == "":
            continue
        if can_print:
            each_line_split = each_line.split()
            if len(each_line_split) != 5:
                raise Exception(f"each cluster line not eq 5,line split:{each_line_split}")
            clusters.append(Cluster(*each_line_split))
    return clusters


# 处理tiup exec cutomer --command ""的结果集
def _tiup_exec_result_to_list(str1):
    """
    tiup exec <cluster_name> --command "xxx"的结果作为str1，返回以(ip,<result>)为元组的列表，输出每一个IP地址上执行的结果
    :param str1:
    :return:
    """
    ipv4_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    re_compile = re.compile(rf"^Run command on.*\nOutputs of.*on ({ipv4_pattern}):\nstdout:\n", re.MULTILINE)
    o_list = re_compile.split(str1)[1:]
    return list(zip(o_list[::2], o_list[1::2]))


# 处理类似于tiup cluster display中标题对应的column起止位置，便于查找对应的值，在组件打patch后会导致值多空格，所以不应采用line.split()来分隔，应用此函数来查找对应的值
def _find_col_start_stop_pos(header_line: str, col_name: str) -> (int, int):
    """
    在标题中找到指定标题的起止位置，便于对该字段的数据进行截取

    Raises ValueError 如果col_names中元素在header_line中不唯一.
    :param header_line:
    :param col_name:
    :param start_pos:header_line中的开始查找位置
    :return:返回col_name在header_line中对应的起止位置

    example:
      在tiup cluster display时列出:ID                    Role          Host等标题，可以用此函数查找某个标题的起止位置

    """
    # col_names中元素应在header_line中唯一
    if header_line.count(col_name) != 1:
        raise ValueError(f"col_name:{col_name}在header_line:{header_line}中必须唯一")
    match = re.compile(rf"\b{col_name}(?:\s+|$)").search(header_line)
    if match:
        return match.start(), match.end()
    else:
        raise ValueError(f"col_name:{col_name}在header_line:{header_line}中找不到起止点")

def _find_json_strings(text):
    """
    从文本中查找json文本，并返回第一个匹配到的json文本对象

    注意： 第一个"{"认为是json的开始，对应的配对的"}"认为json的结束，查找到后则返回，如果字符串中存在无效"{"则可能存在解析错误的问题

    解析： tiup cluster display tidb-test  --format=json形式的结果
    :param text: 传入的字符串
    :return: 返回第一个匹配到的json格式文本
    """
    start = 0
    stack = []
    # 只查找第一个json文本
    first = True
    for i, char in enumerate(text):
        if char == '{':
            stack.append(i)
            if first:
                start = i
                first = False
        elif char == '}':
            if stack:
                stack.pop()
                if not stack:
                    end = i + 1
                    return text[start:end]
    return ""

if __name__ == "__main__":
    str1 = """
    tiup is checking updates for component cluster ...
A new version of cluster is available:
   The latest version:         v1.14.0
   Local installed version:    v1.12.5
   Update current component:   tiup update cluster
   Update all components:      tiup update --all

Starting component `cluster`: /home/tidb/.tiup/components/cluster/v1.12.5/tiup-cluster display tidb-test --format=json
{
  "cluster_meta": {
    "cluster_type": "tidb",
    "cluster_name": "tidb-test",
    "cluster_version": "v7.5.0",
    "deploy_user": "tidb",
    "ssh_type": "builtin",
    "tls_enabled": false,
    "dashboard_url": "http://192.168.31.201:2379/dashboard",
    "grafana_urls": [
      "http://192.168.31.201:3000"
    ]
  },
  "instances": [
    {
      "id": "192.168.31.201:9093",
      "role": "alertmanager",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "9093/9094",
      "os_arch": "linux/x86_64",
      "status": "Up",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "/data/tidb-data/alertmanager-9093",
      "deploy_dir": "/data/tidb-deploy/alertmanager-9093",
      "ComponentName": "alertmanager",
      "Port": 9093
    },
    {
      "id": "192.168.31.201:3000",
      "role": "grafana",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "3000",
      "os_arch": "linux/x86_64",
      "status": "Up",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "-",
      "deploy_dir": "/data/tidb-deploy/grafana-3000",
      "ComponentName": "grafana",
      "Port": 3000
    },
    {
      "id": "192.168.31.201:2379",
      "role": "pd",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "2379/2380",
      "os_arch": "linux/x86_64",
      "status": "Up|L|UI",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "/data/tidb-data/pd-2379",
      "deploy_dir": "/data/tidb-deploy/pd-2379",
      "ComponentName": "pd",
      "Port": 2379
    },
    {
      "id": "192.168.31.201:9090",
      "role": "prometheus",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "9090/12020",
      "os_arch": "linux/x86_64",
      "status": "Up",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "/data/tidb-data/prometheus-9090",
      "deploy_dir": "/data/tidb-deploy/prometheus-9090",
      "ComponentName": "prometheus",
      "Port": 9090
    },
    {
      "id": "192.168.31.201:4000",
      "role": "tidb",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "4000/10080",
      "os_arch": "linux/x86_64",
      "status": "Up",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "-",
      "deploy_dir": "/data/tidb-deploy/tidb-4000",
      "ComponentName": "tidb",
      "Port": 4000
    },
    {
      "id": "192.168.31.201:9000",
      "role": "tiflash",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "9000/8123/3930/20170/20292/8234",
      "os_arch": "linux/x86_64",
      "status": "Up",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "/data/tidb-data/tiflash-9000",
      "deploy_dir": "/data/tidb-deploy/tiflash-9000",
      "ComponentName": "tiflash",
      "Port": 9000
    },
    {
      "id": "192.168.31.201:20160",
      "role": "tikv",
      "host": "192.168.31.201",
      "manage_host": "192.168.31.201",
      "ports": "20160/20180",
      "os_arch": "linux/x86_64",
      "status": "Up",
      "memory": "-",
      "memory_limit": "-",
      "cpu_quota": "-",
      "since": "-",
      "data_dir": "/data/tidb-data/tikv-20160",
      "deploy_dir": "/data/tidb-deploy/tikv-20160",
      "ComponentName": "tikv",
      "Port": 20160
    }
  ]
}
{"exit_code":0}
"""
    print(_find_json_strings(str1))