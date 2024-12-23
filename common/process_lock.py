#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import fcntl
import os
import time
import threading
import atexit


class ProcessLock:
    """
    进程锁类，避免多个进程同时执行同一个脚本。

    使用方式为：
    with ProcessLock("lock_file"):
        # do something

    :param lock_file: 锁文件路径
    """
    def __init__(self, lock_file):
        """
        初始化进程锁。

        :param lock_file: 锁文件路径
        """
        self.lock_file = lock_file
        self.locked = False
        self.keep_alive_interval = 60
        self.__keep_alived = False
        self.__file_descriptor = None
        self.__lock_thread = None
        self.__thread_lock = threading.Lock()
        atexit.register(self.release)

    def acquire(self):
        """
        尝试获取锁。若锁获取成功，启动守护线程；否则抛出异常。

        :return: 如果获取锁成功，返回True；否则返回False
        """
        with self.__thread_lock:
            if self.locked:
                return True

            self.__file_descriptor = os.open(self.lock_file, os.O_RDWR | os.O_CREAT)
            try:
                # 尝试加锁
                fcntl.flock(self.__file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # 写入当前进程PID到锁文件
                os.ftruncate(self.__file_descriptor, 0)
                os.write(self.__file_descriptor, str(os.getpid()).encode())
                self.locked = True
                self.keep_alive()
                return True
            except IOError:
                # 如果无法加锁，检查锁文件状态
                self.__validate_lock_file()
                return False

    def release(self):
        """
        释放锁，清理锁文件。
        """
        with self.__thread_lock:
            if self.locked:
                if self.__file_descriptor is not None:
                    os.close(self.__file_descriptor)
                    self.__file_descriptor = None
                if os.path.isfile(self.lock_file):
                    try:
                        os.remove(self.lock_file)
                    except OSError:
                        pass
                self.locked = False

    def keep_alive(self):
        """
        守护线程定期更新锁文件时间戳，确保锁状态。
        """
        if not self.__keep_alived:
            self.__lock_thread = threading.Thread(target=self.__keep_alive, daemon=True)
            self.__lock_thread.start()
            self.__keep_alived = True

    def __keep_alive(self):
        """
        定期刷新锁文件的mtime。
        """
        while self.locked:
            with self.__thread_lock:
                if self.__file_descriptor is not None:
                    try:
                        os.fsync(self.__file_descriptor)  # 刷新文件状态
                    except OSError:
                        self.release()
                        raise
            time.sleep(self.keep_alive_interval)

    def __validate_lock_file(self):
        """
        检查锁文件是否为残留锁。
        """
        try:
            with open(self.lock_file, 'r') as f:
                pid = int(f.read().strip())
            if not self.process_id_exists(pid):
                # 清理残留锁文件
                os.remove(self.lock_file)
        except (ValueError, OSError):
            # 锁文件损坏或无法读取
            if os.path.isfile(self.lock_file):
                os.remove(self.lock_file)

    @staticmethod
    def process_id_exists(pid):
        """
        检查PID是否存在。

        :param pid: 进程ID
        :return: 如果进程存在，返回True；否则返回False
        """
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def __enter__(self):
        """
        进入上下文管理器，尝试获取锁。

        :return: 返回自身实例
        :raises RuntimeError: 如果获取锁失败
        """
        if not self.acquire():
            raise RuntimeError("Failed to acquire process lock.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文管理器，释放锁。

        :param exc_type: 异常类型
        :param exc_val: 异常值
        :param exc_tb: 异常回溯
        :return: 总是返回False
        """
        self.release()
        return False