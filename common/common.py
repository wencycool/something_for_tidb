# -*- coding: utf-8 -*-
# coding: utf-8

"""
存放常用的公共函数，方便调用
"""

import sys, threading, subprocess, tempfile
import re
import ast

# 判断python的版本
if float(sys.version[:3]) < 3.6:
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
    return None,False


def _tiup_exec_result_to_list(str1):
    """
    tiup exec <cluster_name> --command "xxx"的结果作为str1，返回以(ip,<result>)为元组的列表，输出每一个IP地址上执行的结果
    :param str1:
    :return:
    """
    ipv4_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    re_compile = re.compile(rf"^Run command on.*\nOutputs of.*on ({ipv4_pattern}):\nstdout:\n", re.MULTILINE)
    o_list = re_compile.split(str1)[1:]
    return list(zip(o_list[::2],o_list[1::2]))