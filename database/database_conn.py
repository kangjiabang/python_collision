# database/database_conn.py
import os
import psycopg2
from psycopg2 import pool # 导入 pool 模块
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, Generator
import logging
from dotenv import load_dotenv

# --- 配置和初始化 ---
# 获取当前环境，默认为 development
env = os.getenv("ENV", "development")

# 构造要加载的 .env 文件路径,上两级目录
dotenv_path_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_env_path = os.path.join(dotenv_path_dir, f".env.{env}")
# logging.basicConfig(level=logging.INFO) # 如果 utils.logger 不可用，可以临时用这个
# logger = logging.getLogger(__name__)
# 使用你现有的 logger
from utils.logger import logger

logger.info(f"加载环境变量文件: {dotenv_env_path}")
# 加载环境变量
load_dotenv(dotenv_env_path)

# 从环境变量中读取数据库配置
db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# --- 连接池配置 ---
# 从环境变量中读取连接池配置，设置默认值
MIN_CONN_SIZE = int(os.getenv("DB_MIN_CONN_SIZE", "10")) # 最小连接数
MAX_CONN_SIZE = int(os.getenv("DB_MAX_CONN_SIZE", "80")) # 最大连接数

# 构建连接字符串（过滤掉 None 值）
DB_CONN_STRING = " ".join([f"{k}={v}" for k, v in db_config.items() if v is not None])
logger.info(f"数据库连接字符串 (用于连接池): {DB_CONN_STRING}")
logger.info(f"连接池配置: Min={MIN_CONN_SIZE}, Max={MAX_CONN_SIZE}")

# --- 全局连接池实例 ---
# 声明全局变量，稍后初始化
connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

def init_connection_pool():
    """
    初始化全局连接池。
    应该在应用启动时调用一次。
    """
    global connection_pool
    if connection_pool is None:
        try:
            # 创建 ThreadedConnectionPool
            # cursor_factory=RealDictCursor 不能直接传给 pool，需要在获取连接后设置
            connection_pool = psycopg2.pool.ThreadedConnectionPool(
                MIN_CONN_SIZE,
                MAX_CONN_SIZE,
                DB_CONN_STRING
                # cursor_factory=RealDictCursor # 注意：pool 不直接支持 cursor_factory
            )
            logger.info("✅ 数据库连接池初始化成功")
        except Exception as e:
            logger.error(f"❌ 数据库连接池初始化失败: {e}")
            raise
    else:
        logger.info("⚠️ 数据库连接池已存在，无需重复初始化")

def close_connection_pool():
    """
    关闭全局连接池。
    应该在应用关闭时调用。
    """
    global connection_pool
    if connection_pool:
        try:
            connection_pool.closeall()
            connection_pool = None # 重置为 None
            logger.info("✅ 数据库连接池已关闭")
        except Exception as e:
            logger.error(f"❌ 关闭数据库连接池时出错: {e}")

# --- 使用连接池的上下文管理器 ---
@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    从连接池获取数据库连接的上下文管理器。
    自动处理连接的获取和归还。
    注意：需要在使用此函数前调用 init_connection_pool()。
    """
    global connection_pool
    conn = None
    if connection_pool is None:
        logger.error("❌ 连接池未初始化，请先调用 init_connection_pool()")
        raise RuntimeError("Connection pool not initialized")

    try:
        # 从连接池获取一个连接
        conn = connection_pool.getconn()
        if conn:
            # 设置连接的游标工厂为 RealDictCursor
            # 注意：每次获取连接后都需要设置，因为 pool 本身不管理这个
            conn.cursor_factory = RealDictCursor
            conn.set_client_encoding('UTF8')
            logger.debug("🔄 从连接池获取连接")
            yield conn
        else:
            logger.error("❌ 无法从连接池获取数据库连接")
            raise psycopg2.OperationalError("Failed to get connection from pool")
    except psycopg2.pool.PoolError as pe:
        logger.error(f"❌ 连接池错误: {pe}")
        raise
    except Exception as e:
        logger.error(f"❌ 从连接池获取连接时发生错误: {e}")
        # 如果发生错误，且连接已获取，则立即放回池中
        if conn:
            connection_pool.putconn(conn)
        raise
    finally:
        # 确保连接在使用后被归还到池中
        if conn:
            try:
                # 检查连接是否仍然有效（可选，但推荐）
                # conn.reset() # 重置连接状态，但这可能不总是必要的或推荐的
                # 更安全的做法是检查连接是否处于良好状态再放回
                # 但如果发生错误，可能连接状态已损坏，直接放回可能有问题
                # 一种策略是：如果发生异常，关闭连接并让池创建新连接
                # 但通常 putconn 会处理这些问题。
                connection_pool.putconn(conn)
                logger.debug("🔄 连接已归还到连接池")
            except Exception as putback_error:
                logger.error(f"❌ 将连接归还到连接池时出错: {putback_error}")
                # 如果归还失败，尝试关闭连接以避免泄露
                try:
                    conn.close()
                    logger.warning("⚠️ 连接归还失败，已强制关闭连接")
                except Exception as close_error:
                     logger.error(f"❌ 强制关闭连接时也出错: {close_error}")

# --- （可选）简化版获取连接函数（不推荐用于需要自动关闭的场景）---
# 如果你需要一个简单的函数来获取连接（例如在某些特定场景下），
# 你仍然需要手动调用 connection_pool.putconn(conn) 来归还连接。
# 但通常推荐使用上下文管理器 get_db_connection。
# def get_db_connection_simple():
#     """
#     简单从连接池获取数据库连接（需手动归还）。
#     注意：必须手动调用 connection_pool.putconn(conn) 或 conn.close()。
#     """
#     global connection_pool
#     if connection_pool is None:
#         raise RuntimeError("Connection pool not initialized")
#     try:
#         conn = connection_pool.getconn()
#         if conn:
#             conn.cursor_factory = RealDictCursor
#             conn.set_client_encoding('UTF8')
#             return conn
#         else:
#             raise psycopg2.OperationalError("Failed to get connection from pool")
#     except Exception as e:
#         logger.error(f"Error getting connection from pool: {e}")
#         raise
