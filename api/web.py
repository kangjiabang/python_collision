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
from utils.logger import logger

# 获取当前文件所在目录的上一级目录
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

app = FastAPI(title="3D Building Collision Detector", openapi_prefix="/api/v1")

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