#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# coding: utf-8

"""
存放常用的公共函数，方便调用
"""

import sys, threading, subprocess, tempfile
import re
import ast
import logging
from logging.handlers import TimedRotatingFileHandler

# 判断python的版本
if sys.version_info < (3, 6):
    raise "python version need larger than 3.6"


def command_run(command, use_temp=False, timeout=30, stderr_to_stdout=True) -> (str, str, int):
    """

    :param str command: shell命令
    :param bool use_temp: 是否使用临时文件存储结果集，对于大结果集处理有效
    :param int timeout: 函数执行超时时间
    :param stderr_to_stdout: 是否将错误输出合并到stdout中
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
            # 标准输出结果集比较大输出到文件，错误输出到PIPE
            mutable[2] = subprocess.Popen(command, stdout=out_fileno, stderr=subprocess.PIPE, shell=True)
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
        if stderr_to_stdout:
            return _str(result) + _str(mutable[2].stderr.read()), mutable[2].returncode
        else:
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
        if stderr_to_stdout:
            return _str(mutable[0]) + _str(mutable[1]), mutable[2].returncode
        else:
            return _str(mutable[0]), mutable[2].returncode


# 获取日志对象，每天生成一个日志文件，最多保存7个日志文件
def get_logger(log_file, level: logging.INFO) -> logging.Logger:
    # 生成文档说明
    """
    :param log_file: 日志文件名
    :param level: 日志级别
    :return: 日志对象

    # 添加示例
    >>> import common
    >>> logger = common.get_logger("test.log", logging.INFO)
    >>> logger.info("test")

    """

    backup_count = 7
    # 创建日志对象，保存在logs目录下，日志文件名为test.log，日志文件大小为1M，最多保存3个日志文件，日志文件编码为utf-8
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    # interval=1，每天生成一个日志文件，interval=2，每隔一天生成一个日志文件
    handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=backup_count,
                                       encoding='utf-8')
    # 以时间为归档后缀
    handler.suffix = "%Y-%m-%d-%H.%M.%S"
    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s-%(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # 添加到日志对象中
    logger.addHandler(handler)
    return logger


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


def check_bool(s):
    """
    检查当前字符串是否布尔类型，并返回布尔值
    :param s:
    :return: （布尔值，是否布尔值）
    """
    if s.lower() == "true":
        return True, True
    elif s.lower() == "false":
        return False, True
    else:
        return None, False

def check_ip(s):
    """
    检查当前字符串是否IP地址，并返回IP地址
    :param s:
    :return: （IP地址，是否IP地址）
    """
    if re.match(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', s):
        return s, True
    else:
        return None, False

def check_dict(s):
    """
    检查当前字符串是否字典类型，并返回字典
    :param s:
    :return: （字典，是否字典）
    """
    try:
        result = ast.literal_eval(s)
        if isinstance(result, dict):
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
    re_split = re_compile.split(str1)
    if len(re_split) == 1:
        return
    o_list = re_split[1:]
    return list(zip(o_list[::2], o_list[1::2]))


# 格式化tiup cluster exec xxx --command "xxx"的结果集，返回以(ip,<result>)为元组的列表，输出每一个IP地址上执行的结果
def tiup_cluster_exec(cmd):
    """
    tiup cluster exec <cluster_name> --command "xxx"的结果作为str1，返回以(ip,<result>)为元组的列表，输出每一个IP地址上执行的结果
    :param cmd:
    :return:
    """
    result, recode = command_run(cmd, stderr_to_stdout=False)
    if recode != 0:
        raise Exception(result)
    return _tiup_exec_result_to_list(result)


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
    log = get_logger("test.log", logging.INFO)
    log.info("hello world")