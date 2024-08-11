import logging
import sys
from colorama import Fore, Style


class Logger:
    """
    自定义日志类，支持控制台和文件输出，支持颜色
    使用方法说明：
    ```python
    >>> from log import Logger as lg
    >>> import logging

    >>> # 初始化日志
    >>> lg.init_logger(level=logging.INFO, enable_log=True, log_file='app.log')

    >>> # 使用日志
    >>> lg.logger.info("This is an info message")
    >>> lg.logger.error("This is an error message")
    ```
    """
    _logger_initialized = False
    logger: logging.Logger = None

    @staticmethod
    def init_logger(level=logging.DEBUG, enable_log=True, log_file=None):
        if Logger._logger_initialized:
            return

        # 定义日志格式
        formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        # 创建控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)

        # 设置全局 logger
        handlers = [console_handler]

        # 如果指定了日志文件，添加文件处理器
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            file_handler.setLevel(level)
            handlers.append(file_handler)

        logging.basicConfig(level=level, handlers=handlers)

        # 配置颜色
        logging.addLevelName(logging.DEBUG, f"{Fore.BLUE}{logging.getLevelName(logging.DEBUG)}{Style.RESET_ALL}")
        logging.addLevelName(logging.INFO, f"{Fore.GREEN}{logging.getLevelName(logging.INFO)}{Style.RESET_ALL}")
        logging.addLevelName(logging.WARNING, f"{Fore.YELLOW}{logging.getLevelName(logging.WARNING)}{Style.RESET_ALL}")
        logging.addLevelName(logging.ERROR, f"{Fore.RED}{logging.getLevelName(logging.ERROR)}{Style.RESET_ALL}")
        logging.addLevelName(logging.CRITICAL,
                             f"{Fore.MAGENTA}{logging.getLevelName(logging.CRITICAL)}{Style.RESET_ALL}")

        # 如果不启用日志，则禁用所有日志
        if not enable_log:
            logging.disable(logging.CRITICAL)

        # 设置全局 logger
        Logger.logger = logging.getLogger("CustomLogger")

        Logger._logger_initialized = True
