# api/web.py
import os
import sys

import psycopg2
from fastapi import FastAPI, Query, HTTPException
# 导入业务逻辑模块
# 导入数据库连接工具
from database.database_conn import get_db_connection
from service.buildings_service import update_all_buildings_info_batch

from service.collision_service import get_collision_buildings_info
from service.buildings_service_file import insert_buildings_from_file
from utils.logger import logger

# 获取当前文件所在目录的上一级目录
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from database.database_conn import init_connection_pool, close_connection_pool # 导入初始化和关闭函数

# --- 初始化连接池 ---
# 在创建 FastAPI 应用实例之前调用
try:
    init_connection_pool()
except Exception as e:
    # 如果连接池初始化失败，应用可能无法正常工作
    print(f"Failed to initialize database connection pool: {e}")
    # 根据你的错误处理策略，可以选择退出应用
    # sys.exit(1) # 取消注释以在初始化失败时退出
    raise # 或者重新抛出异常


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时的逻辑 (已经移到上面了，但如果需要在 lifespan 内部做，可以放这里)
    # init_connection_pool() # 如果上面没有调用，可以在这里调用
    print("Application startup complete.")
    yield # 应用运行期间
    # 关闭时的逻辑
    close_connection_pool()
    print("Application shutdown complete.")

# 使用 lifespan 参数创建 FastAPI 应用
app = FastAPI(title="3D Building Collision Detector", openapi_prefix="/api/v1", lifespan=lifespan)
@app.post("/update_buildings_info")
async def update_buildings_info ():
    """
    更新建筑物信息。
    """
    try:
        # 使用原有逻辑进行碰撞检测
        #with get_db_connection_simple() as conn:
        conn = psycopg2.connect(
            host="localhost",
            database="nyc",
            user="postgres",
            password="123456"
        )
        result = update_all_buildings_info_batch(conn)
        return  result

    except Exception as e:
        logger.error(f"更新建筑物时发生错误: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"更新建筑物时发生错误: {str(e)}"
            }
        )


@app.get("/collision_info")
async def collision_info(
    longitude: float = Query(..., description="经度（WGS84）"),
    latitude: float = Query(..., description="纬度（WGS84）"),
    height: float = Query(..., description="高度（米）"),
    collision_distance : float = Query(2, description="高度（米），默认距离为2米"),
):
    """
    根据传入的 WGS84 经纬高判断是否与建筑物发生碰撞。
    返回结果包含:
    - is_collision: 是否发生碰撞 (true/false)
    - point: 查询点的坐标
    - collisions: 碰撞的建筑物列表 (如果发生碰撞)
    """
    try:

        logger.info(f"经纬度和高度: {longitude}, {latitude}, {height}, 碰撞距离: {collision_distance}")

        # 使用原有逻辑进行碰撞检测
        with get_db_connection() as conn:
            result = get_collision_buildings_info(conn, longitude, latitude, height, collision_distance)

        # 判断是否有碰撞结果
        is_collision = len(result) > 0

        response = {
            "status": "success",
            "is_collision": is_collision,
        }

        # 只有当有碰撞时才返回collisions字段
        if is_collision:
            response["building_infos"] = result

        return response

    except Exception as e:
        logger.error(f"检测碰撞时发生错误: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"检测碰撞时发生错误: {str(e)}"
            }
        )
@app.post("/insert_buildings_info")
async def insert_buildings_info(
    file_path: str = Query(..., description="文件路径）"),
):
    """
     导入建筑物信息
    """
    try:

        logger.info(f"文件路径:{file_path}")

        # 使用原有逻辑进行碰撞检测
        with get_db_connection() as conn:
            result = insert_buildings_from_file(conn,file_path)

        if result:
            return result

        return {
            "success": False,
            "code": 500,
            "errorMsg": "未知错误"
        }

    except Exception as e:
        logger.error(f"导入建筑物信息发生错误: {str(e)}")

        return {
            "success": False,
            "code": 501,
            "errorMsg": "导入时发生异常"
        }