import psycopg2
import requests


def update_all_buildings_info_batch(conn):
    """
    分批处理所有建筑物，避免内存占用过大
    """
    batch_size = 1000  # 每批处理1000条
    offset = 0
    total_processed = 0
    total_success = 0
    total_errors = 0

    while True:
        try:
            cur = conn.cursor()
            # 分批查询
            # 修正的查询语句 - 使用几何类型的有效性检查
            cur.execute("""
                            SELECT gid, ST_AsText(geom) as geom_text 
                            FROM hz_yuhang_buildings 
                            WHERE geom IS NOT NULL 
                            AND ST_IsValid(geom)  -- 检查几何是否有效
                            AND NOT ST_IsEmpty(geom)  -- 检查几何是否非空
                            ORDER BY gid
                            LIMIT %s OFFSET %s
                        """, (batch_size, offset))

            buildings = cur.fetchall()

            # 如果没有更多数据，退出循环
            if not buildings:
                print("所有建筑物处理完成")
                break

            print(f"开始处理第 {offset // batch_size + 1} 批，共 {len(buildings)} 个建筑物")

            # 处理这一批数据
            processed_count, success_count, error_count = process_building_batch(conn, buildings)

            total_processed += processed_count
            total_success += success_count
            total_errors += error_count

            print(f"第 {offset // batch_size + 1} 批处理完成: 成功{success_count}, 失败{error_count}")

            # 提交这一批的更改
            conn.commit()

            # 移动到下一批
            offset += batch_size

            # 关闭游标
            cur.close()

            # 可选：添加延迟避免API过载
            # time.sleep(1)

        except Exception as e:
            print(f"处理第 {offset // batch_size + 1} 批时出错: {str(e)}")
            if 'cur' in locals():
                cur.close()
            conn.rollback()
            break

    # 输出最终统计
    print(f"\n全部处理完成!")
    print(f"总计处理: {total_processed}")
    print(f"成功更新: {total_success}")
    print(f"处理失败: {total_errors}")
    print(f"总体成功率: {total_success / total_processed * 100:.1f}%" if total_processed > 0 else "0%")
    return {
        "total_processed": total_processed,
        "total_success": total_success,
        "total_errors": total_errors,
        "success_rate": total_success / total_processed * 100 if total_processed > 0 else 0
    }


def process_building_batch(conn, buildings):
    """
    处理一批建筑物数据
    """
    cur = conn.cursor()
    processed_count = 0
    success_count = 0
    error_count = 0

    for gid, geom_text in buildings:
        try:
            processed_count += 1

            # 获取建筑物中心点
            centroid_query = f"""
                SELECT ST_AsText(ST_Centroid(ST_GeomFromText(%s, 4326)))
            """

            cur.execute(centroid_query, (geom_text,))
            centroid_result = cur.fetchone()

            if not centroid_result or not centroid_result[0]:
                print(f"_gid {gid}: 无法获取中心点")
                error_count += 1
                continue

            centroid_wkt = centroid_result[0]

            # 解析POINT坐标
            if centroid_wkt.startswith('POINT(') and centroid_wkt.endswith(')'):
                coords_str = centroid_wkt[6:-1]
                longitude, latitude = map(float, coords_str.split())

                # 调用API获取高度
                api_url = f"http://localhost:3100/api/get_building_height?longitude={longitude}&latitude={latitude}"

                try:
                    response = requests.get(api_url, timeout=10)
                    response.raise_for_status()

                    data = response.json()

                    if data.get('success') and 'height' in data:
                        building_height = float(data['height'])

                        # 更新数据库
                        update_query = """
                            UPDATE hz_yuhang_buildings 
                            SET building_height = %s 
                            WHERE gid = %s
                        """

                        cur.execute(update_query, (building_height, gid))

                        print(f"✓ gid {gid}: 中心点({longitude:.6f}, {latitude:.6f}), 高度={building_height}米")
                        success_count += 1

                    else:
                        print(f"✗ gid {gid}: API无有效数据")
                        error_count += 1

                except requests.exceptions.RequestException as e:
                    print(f"✗ gid {gid}: API请求失败 - {str(e)}")
                    error_count += 1
                except (ValueError, KeyError) as e:
                    print(f"✗ gid {gid}: 数据解析错误 - {str(e)}")
                    error_count += 1

            else:
                print(f"✗ gid {gid}: 坐标格式错误")
                error_count += 1

        except Exception as e:
            print(f"✗ gid {gid}: 处理错误 - {str(e)}")
            error_count += 1
            continue

    cur.close()
    return processed_count, success_count, error_count


# 使用示例：
if __name__ == "__main__":
    # 数据库连接示例（请根据实际情况修改）
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="nyc",
            user="postgres",
            password="123456"
        )

        # 执行更新
        update_all_buildings_info_batch(conn)

        # 关闭连接
        conn.close()

    except Exception as e:
        print(f"数据库连接失败: {str(e)}")