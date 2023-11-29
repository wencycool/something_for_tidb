# -*- coding: utf-8 -*-
# coding: utf-8

"""
存放常用的公共函数，方便调用
"""

import sys, threading, subprocess, tempfile

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

