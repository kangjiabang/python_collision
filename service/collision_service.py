import time  # 导入 time 模块
from psycopg2.extras import RealDictCursor
from typing import List
from utils.logger import logger


def get_collision_buildings_info(conn, longitude: float, latitude: float, height: float, collision_distance: float) -> \
List[dict]:
    """
    判断点是否与某栋建筑发生碰撞。
    返回匹配的建筑物列表。
    """
    query = """
        SELECT 
            osm_id, name, ST_AsText(geom) AS geom, building_height
        FROM 
            hz_yuhang_buildings
        WHERE 
            ST_DWithin(
                geom::geography, 
                ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography, %(collision_distance)s)
            AND %(height)s < building_height
    """

    params = {
        "longitude": longitude,
        "latitude": latitude,
        "height": height,
        "collision_distance": collision_distance
    }

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. 记录查询开始时间
        start_time = time.time()

        # 获取并打印完整SQL
        full_sql = cur.mogrify(query, params)
        logger.info("Executing SQL:\n%s", full_sql.decode('utf-8'))

        # 执行查询
        cur.execute(query, params)
        result = cur.fetchall()  # 获取结果

        # 2. 记录查询结束时间
        end_time = time.time()

        # 3. 计算并打印查询耗时
        execution_time = end_time - start_time
        logger.info("Database query executed in %.4f seconds", execution_time)

        # 4. 返回结果
        return result
