#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
进程锁，避免多个进程同时执行同一个脚本
使用方式为：
with ProcessLock("lock_file"):
    # do something
"""

import os
import time
import threading
import atexit


class ProcessLock:
    """
    制定锁注册机制，避免一个脚本重复多次执行，给定一个锁文件，如果锁文件存在则判定为脚本已经在执行，否则判定为脚本未执行
    机制为：
    在程序获取锁之前：
    1、如果锁文件不存在，则创建锁文件，获锁成功
    2、如果锁文件存在，且锁文件中的pid在当前进程列表中不存在，则判定为残留锁文件，清理之，获锁成功
    3、如果锁文件存在，且锁文件中的pid在当前进程列表中存在，但是mtime超过20小时未更新，则判定为残留锁文件，清理之，获锁成功
    4、其余情况判定为锁文件被当前进程占用，获取锁失败
    在程序获取锁之后：
    1、定期更新锁文件的mtime为当前时间
    2、如果文件不存在或文件中pid不是当前进程id，则抢锁（__acquire_attempts），并维护锁文件
    3、抢锁次数不超过max_acquire_attempts次,超过后抛出异常
    """

    def __init__(self, lock_file):
        self.lock_file = lock_file
        self.locked = False
        # 避免极端情况下篡改锁文件内容导致多进程相互抢锁，这里限制抢锁次数不得超过10次
        self.max_acquire_attempts = 10
        # keepalive的探测周期,默认60秒
        self.keep_alive_interval = 60
        self.__acquire_attempts = 0
        self.__keep_alived = False
        self.atexit()

    # 当进程退出时，清理锁文件
    def atexit(self):
        atexit.register(self.release)

    def acquire(self):
        if self.__acquire():
            self.locked = True
            self.keep_alive()
            return True
        return False

    # 尝试获取锁，如果获取成功则返回True，否则返回False
    def __acquire(self):
        if self.locked:
            return True
        elif os.path.isfile(self.lock_file):
            # 1、如果锁文件存在，且内容是当前进程的pid，则判断为锁文件被当前进程占用，返回True
            if str(os.getpid()) == open(self.lock_file).read():
                # 虽然pid在操作系统当前进程中存在但是不一定是“当前脚本的”，因此结合mtime进一步判断是否脚本的mtime有定期更新，没有定期更新说进程确实不存在
                # 为避免误操作，这里谨慎处理，如果文件未变化超过20小时则说明确实不存在,清理文件锁
                if time.time() - os.path.getmtime(self.lock_file) > 20 * 3600:
                    os.remove(self.lock_file)
                    return self.acquire()
                return True
            # 2、如果文件锁存在，且文件中pid在当前进程列表中不存在，则判定为残留锁文件，清理之
            try:
                infile_pid = int(open(self.lock_file).read())
                if not self.process_id_exists(infile_pid):
                    os.remove(self.lock_file)
                    return self.acquire()
                else:
                    return False
            except ValueError:
                # 说明锁文件中的pid不是数字，清理之
                os.remove(self.lock_file)
                return self.acquire()
        else:
            # 如果锁文件不存在，则创建锁文件
            with open(self.lock_file, "w") as f:
                # 写入当前进程号的pid
                f.write(str(os.getpid()))
            return True

    def release(self):
        if self.locked:
            os.remove(self.lock_file)
            self.locked = False

    # 启动一个线程在后台不停的修改self.lock_file的mtime，防止别人篡改锁文件
    def keep_alive(self):
        if not self.__keep_alived:
            t = threading.Thread(target=self.__keep_alive)
            t.setDaemon(True)
            t.start()
            self.__keep_alived = True

    def __keep_alive(self):
        """
        每分钟检测一次锁文件是否存在，不存在创建之，存在pid不一致则更新之
        """
        while self.locked:
            if self.max_acquire_attempts < self.__acquire_attempts:
                self.release()
                raise Exception(f"获取锁失败，尝试次数超过{self.max_acquire_attempts}次")
            if os.path.isfile(self.lock_file):
                # 如果当前进程号不在锁文件中，更新锁文件
                if str(os.getpid()) != open(self.lock_file).read():
                    with open(self.lock_file, "w") as f:
                        # 写入当前进程号的pid
                        f.write(str(os.getpid()))
                    self.__acquire_attempts += 1
                # 定期更新锁文件的mtime为当前时间
                else:
                    os.utime(self.lock_file, None)
            else:
                with open(self.lock_file, "w") as f:
                    # 写入当前进程号的pid
                    f.write(str(os.getpid()))
                self.__acquire_attempts += 1
            time.sleep(self.keep_alive_interval)

    # 检查进程号是否存在，如果不存在则返回False，否则返回True
    def process_id_exists(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    # 支持with语法
    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
