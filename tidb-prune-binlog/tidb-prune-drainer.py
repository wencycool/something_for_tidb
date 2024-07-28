# !/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
- 保证日志完全被集中备份备走才可清理
- 在满足使用率告警的前提下，尽可能多的保留日志，用于排查问题和备库重建等任务
- 当文件系统使用率超过80%时，自动将其清理到70%以下
- 跑批判定：当评估最近一个小时内日志增长量超过NGB（或使用率增长快），自动将其清理到60%以下
- 预先计算：按照日志顺序，逐次计算最早（最该清理）的日志文件大小，直到降到到目标使用率后停止计算，这些纳入计算的日志为清理目标
- 在正常情况下，每30分钟执行一次清理，清理24小时前日志。
"""
import os
import shutil
import time
import logging as log
from logging.handlers import TimedRotatingFileHandler
import re
import argparse


# 将字节自适应转为KB、MB、GB、TB
def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0


# 判断一个文件是否被别的进程打开,这里因为需要引入第三方依赖psutil，所以暂时不用
"""
import os
import psutil
import time

def is_file_locked(file_path):
    try:
        # 尝试打开文件以获取文件句柄
        with open(file_path, 'a') as f:
            pass
        return False  # 文件未被占用
    except IOError:
        return True  # 文件被占用

def clean_locked_file(file_path):
    if is_file_locked(file_path):
        print(f"File {file_path} is locked by another process. Cleaning is ineffective.")
        # 在这里执行清理操作，例如删除文件、备份文件等
    else:
        print(f"File {file_path} is not locked. Cleaning can proceed.")

# 示例用法
file_to_clean = 'path/to/your/file.txt'

# 检查文件是否被占用
if is_file_locked(file_to_clean):
    print(f"File {file_to_clean} is currently locked by another process.")
else:
    print(f"File {file_to_clean} is not locked.")

# 进行清理操作（如果文件未被占用）
clean_locked_file(file_to_clean)

"""


# 查看一个文件（或目录）所在挂载点使用情况
def get_mount_point_usage(path):
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return shutil.disk_usage(path)


# 从所有可清理的归档日志文件中，按照时间顺序，找出最早的一批文件，当可保证挂载点使用率低于阈值时，返回这批文件
def get_archive_log_files_to_prune(files, threshold):
    """
    找出可让挂载点使用率低于阈值的一批文件
    :param files: 目标文件列表
    :param threshold: 阈值（如0.6表示当前挂载点使用率低于60%时，返回文件列表）
    :return: 返回文件列表
    """
    # 当前挂载点使用率
    mount_point_usage = round(get_mount_point_usage(files[0]).used / get_mount_point_usage(files[0]).total, 2)
    log.info(f"当前挂载点使用率：{mount_point_usage}")
    log.info(f"目标挂载点使用率阈值：{threshold}")
    for each_file in files:
        if not os.path.isfile(each_file):
            raise Exception("不是文件")
    files.sort(key=lambda x: os.path.getmtime(x))
    files_to_prune = []
    files_to_prune_size = 0
    for each_file in files:
        each_file_size = os.path.getsize(each_file)
        files_to_prune_size += each_file_size
        if (get_mount_point_usage(each_file).used - files_to_prune_size) / get_mount_point_usage(
                each_file).total < threshold:
            return files_to_prune
        else:
            files_to_prune.append(each_file)
    return files_to_prune


# 删除这批文件
def prune_files(files):
    log.info("删除文件列表：")
    for each_file in files:
        try:
            os.remove(each_file)
            log.info(f"删除文件成功：{each_file}")
        except Exception as e:
            log.error(f"删除文件失败：{each_file}，异常：{e}")


# 从一批文件的列表中，按照时间顺序判断增长速度，如果增长速度超过阈值，则判定为批次清理任务
def is_app_prune_task(files, threshold_per_hour):
    """
    判断是否为应用批次清理任务导致归档日志增长较快
    :param files: 文件列表
    :param threshold_per_hour: 每小时导致挂载点使用率增长的阈值，如0.1表示每小时导致挂载点使用率增长10%
    :return: 返回文件列表
    """
    log.info(f"判断是否为应用批次清理任务导致归档日志增长较快，最近1小时增长量阈值：{threshold_per_hour}")
    # 从文件列表中判断出最近一小时平均增长速度，如果超过阈值，则判定为批次清理任务
    files.sort(key=lambda x: os.path.getmtime(x))
    files_in_last_hour = []
    # todo 日志保留时间必须超过一小时才能评估准确
    for each_file in files:
        if os.path.getmtime(each_file) > time.time() - 3600:
            files_in_last_hour.append(each_file)
    if len(files_in_last_hour) > 1:
        files_in_last_hour.sort(key=lambda x: os.path.getmtime(x))
        # 最近一个小时产生文件总大小
        files_in_last_hour_total_size = sum([os.path.getsize(each_file) for each_file in files_in_last_hour])
        log.info(f"最近1小时产生文件总大小：{convert_bytes(files_in_last_hour_total_size)}")
        # 挂载点总大小
        mount_point_total_size = get_mount_point_usage(files[0]).total
        # 最近一个小时挂载点使用率增长百分比
        mount_point_usage_growth_rate = round(files_in_last_hour_total_size / mount_point_total_size, 2)
        if mount_point_usage_growth_rate > threshold_per_hour:
            log.info(f"最近1小时挂载点使用率增长百分比：{mount_point_usage_growth_rate}, 超过阈值：{threshold_per_hour}, "
                     f"判断为应用批次清理任务导致归档日志增长较快")
            return True
        else:
            log.info(
                f"最近1小时挂载点使用率增长百分比：{mount_point_usage_growth_rate}, 未超过阈值：{threshold_per_hour}, "
                f"判断为非应用批次清理任务导致归档日志增长较快")
            return False
    log.info("最近1小时产生文件数量小于2，无法评估是否为应用批次清理任务导致归档日志增长较快")
    return False


# 获取日志对象，每天生成一个日志文件，最多保存7个日志文件
def get_logger(log_file, level: log.INFO) -> log.Logger:
    # 生成文档说明
    """
    :param log_file: 日志文件名
    :param level: 日志级别
    :return: 日志对象

    """

    backup_count = 7
    # 创建日志对象，保存在logs目录下，日志文件名为test.log，日志文件大小为1M，最多保存3个日志文件，日志文件编码为utf-8
    logger = log.getLogger(__name__)
    logger.setLevel(level)
    if log_file is None:
        # 日志stdout输出
        handler = log.StreamHandler()
    else:
        # interval=1，每天生成一个日志文件，interval=2，每隔一天生成一个日志文件
        handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=backup_count,
                                           encoding='utf-8')
        # 以时间为归档后缀
        handler.suffix = "%Y-%m-%d-%H.%M.%S"
    # 设置日志格式
    formatter = log.Formatter('%(asctime)s - %(name)s-%(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # 添加到日志对象中
    logger.addHandler(handler)
    return logger


# 判断当前目录所在挂在点使用率是否超过阈值，如果超过阈值
def is_mount_point_usage_exceed_threshold(path, threshold):
    """
    判断当前目录所在挂在点使用率是否超过阈值
    :param path: 目标目录
    :param threshold: 如果超过阈值则返回True
    :return: True or False
    """
    mount_point_usage = round(get_mount_point_usage(path).used / get_mount_point_usage(path).total, 2)
    if mount_point_usage > threshold:
        log.info(f"当前挂载点使用率：{mount_point_usage}, 超过阈值：{threshold}")
        return True
    else:
        return False


def do_prune_arichve_logs(archlog_path, file_pattern, threshold1, threshold2, threshold_per_hour, preview=False):
    """
    执行归档日志清理任务
    :param archlog_path: 归档日志目录
    :param file_pattern: 归档日志文件名模式（支持正则表达式）
    :param threshold1: 正常保持使用率阈值，经过清理后，挂载点使用率不会超过这个阈值
    :param threshold2: 批次时段使用率阈值，经过清理后，挂载点使用率不会超过这个阈值，被判定为批次时段后阈值应更低，对批次容忍度更高
    :param threshold_per_hour: 每小时导致挂载点使用率增长的阈值，如0.1表示每小时导致挂载点使用率增长10%，如果最近1小时超过该阈值，则判定为批次清理任务
    :param preview: 是否为预览模式，预览模式下不会删除文件，只打印日志
    :return: 文件清理失败列表，文件系统当前使用率和总耗时
    """
    t1 = time.time()
    # 在archlog_path目录下，找出所有符合file_pattern的文件
    files = []
    pattern = re.compile(rf"{file_pattern}")
    for each_file in os.listdir(archlog_path):
        if pattern.search(each_file):
            abc_filename = os.path.join(archlog_path, each_file)
            # 如果确实是文件则加入列表
            if os.path.isfile(abc_filename):
                files.append(abc_filename)
    threshold = threshold1
    dirname = os.path.dirname(files[0])
    if not os.path.isdir(dirname):
        raise Exception(f"{dirname}不是目录")
    # 判断是否为应用批次清理任务导致归档日志增长较快
    if is_app_prune_task(files, threshold_per_hour):
        log.info("判断为应用批次清理任务导致归档日志增长较快，不执行归档日志清理任务")
        threshold = threshold2

    # 找出可让挂载点使用率低于阈值的一批文件
    files_to_prune = get_archive_log_files_to_prune(files, threshold)
    if preview:
        # 计算文件总大小
        files_to_prune_total_size = sum([os.path.getsize(each_file) for each_file in files_to_prune])
        # 预计删除文件后挂载点使用率
        mount_point_usage = round(
            (get_mount_point_usage(dirname).used - files_to_prune_total_size) / get_mount_point_usage(dirname).total, 2)
        log.info(
            f"预览模式，不删除文件，只打印日志，预计删除文件总大小：{convert_bytes(files_to_prune_total_size)},预计删除文件后挂载点使用率：{mount_point_usage}")
        t2 = time.time()
        return files_to_prune, mount_point_usage, round(t2 - t1, 2)
    else:
        # 删除这批文件
        prune_files(files_to_prune)
        t2 = time.time()
        # 当前挂载点使用率
        mount_point_usage = round(get_mount_point_usage(dirname).used / get_mount_point_usage(dirname).total, 2)
        log.info(f"当前挂载点使用率：{mount_point_usage}, 耗时：{round(t2 - t1, 2)}秒")
        return files_to_prune, mount_point_usage, round(t2 - t1, 2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="归档日志清理脚本")
    parser.add_argument("--arclog-path", type=str, help="归档日志目录", required=True)
    parser.add_argument("--file-pattern", type=str, help="归档日志文件名模式（支持正则表达式）", required=True)
    parser.add_argument("--alarm-threshold", type=float, help="文件系统使用率告警阈值", default=0.8)
    parser.add_argument("--normal-threshold", type=float, help="正常保持使用率阈值", default=0.7)
    parser.add_argument("--batch-threshold", type=float,
                        help="批次时段使用率阈值，经过清理后，挂载点使用率不会超过这个阈值，被判定为批次时段后阈值应更低，对批次容忍度更高",
                        default=0.6)
    parser.add_argument("--threshold-per-hour", type=float,
                        help="每小时导致挂载点使用率增长的阈值，如0.1表示每小时导致挂载点使用率增长10%%，如果最近1小时超过该阈值，则判定为批次清理任务",
                        default=0.1)
    parser.add_argument("--log-file", type=str, help="日志文件名", default="prune_archive.log")
    parser.add_argument("--log-level", type=str, help="日志级别", default="INFO")
    # 日志输出到文件还是前端
    parser.add_argument("--log-to-file", action="store_true", help="日志输出到文件还是前端")
    # 加上preview参数，预览模式下不会删除文件，只打印日志
    parser.add_argument("--preview", action="store_true", help="是否为预览模式，预览模式下不会删除文件，只打印日志")
    args = parser.parse_args()
    if args.log_to_file:
        log = get_logger(args.log_file, args.log_level)
    else:
        log = get_logger(None, args.log_level)
    arclog_path = args.arclog_path
    file_pattern = args.file_pattern
    alarm_threshold = args.alarm_threshold  # 文件系统使用率告警阈值
    normal_threshold = args.normal_threshold  # 正常保持使用率阈值
    batch_threshold = args.batch_threshold  # 批次时段使用率阈值，经过清理后，挂载点使用率不会超过这个阈值，被判定为批次时段后阈值应更低，对批次容忍度更高
    threshold_per_hour = args.threshold_per_hour  # 每小时导致挂载点使用率增长的阈值，如0.1表示每小时导致挂载点使用率增长10%，如果最近1小时超过该阈值，则判定为批次清理任务

    preview = args.preview  # 是否为预览模式，预览模式下不会删除文件，只打印日志
    if is_mount_point_usage_exceed_threshold(arclog_path, alarm_threshold):
        if preview:
            log.debug(f"预览模式，挂载点使用率超过阈值：{alarm_threshold},不执行归档日志清理任务")
        else:
            log.debug(f"挂载点使用率超过阈值：{alarm_threshold}，执行归档日志清理任务")
        do_prune_arichve_logs(arclog_path, file_pattern, normal_threshold, batch_threshold, threshold_per_hour,
                              preview=preview)
