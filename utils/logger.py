
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(log_file='logs/app.log', level=logging.INFO):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # ✅ 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        # 文件处理器（带滚动）
        file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# 初始化全局 logger
logger = setup_logger()