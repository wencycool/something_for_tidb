# 限制python的内存使用不超过8GB


def set_max_memory(max_memory=8 * 1024 * 1024 * 1024):
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

# 将任何一个类转转到sqlite3的数据表

