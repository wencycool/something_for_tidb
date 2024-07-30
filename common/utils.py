import logging as log
import sys


def with_timeout(timeout, func, *args, **kwargs):
    """
    给函数添加超时处理，超过timeout秒则抛出异常，用于控制当前脚本核心函数的执行时间
    内存控制最大为5GB，超过5GB会抛出MemoryError

    :param timeout: 超时时间，单位为秒
    :type timeout: int
    :param func: 待执行的函数
    :type func: function
    :param args: 函数位置参数
    :type args: tuple
    :param kwargs: 函数关键字参数
    :type kwargs: dict
    :rtype: None
    :Examples:
    >>> def sample_function(x, y):
    >>>     return x + y
    >>> with_timeout(1, sample_function, 1, 2)
    3
    """
    if not sys.platform == 'linux':
        return func(*args, **kwargs)
    import resource
    # 为避免对象过多，限制真实物理内存为5GB，如果超过5GB，会抛出MemoryError
    try:
        resource.setrlimit(resource.RLIMIT_RSS, (5368709120, 5368709120))
    except Exception as e:
        log.warning(f"setrlimit failed, error: {e}")
        exit(1)
    import signal
    def timeout_handler(signum, frame):
        raise Exception("timeout")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    try:
        func(*args, **kwargs)
    except Exception as e:
        log.warning(f"analyze failed, error: {e}")
    finally:
        signal.alarm(0)


