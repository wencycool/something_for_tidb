# 限制python的内存使用不超过8GB
import logging

def set_max_memory(max_memory=4 * 1024 * 1024 * 1024):
    """
    限制python的内存使用不超过max_memory，异步操作，不会阻塞后续代码执行
    :param max_memory: 最大内存限制
    :type max_memory: int
    """
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (max_memory, max_memory))
    except ImportError:
        return


# 装饰器来给函数设置异常处理
def catch_exception(func):
    """使用说明：
    @catch_exception
    def func():
        pass
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(e)
            return None
    return wrapper