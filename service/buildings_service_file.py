import time
import psycopg2
import requests
import hashlib
import geohash2  # 需要安装: pip install geohash2
import json  # 用于处理 WKT 解析可能需要的辅助

# 尝试导入 shapely 来计算中心点，如果失败则回退到简单方法
try:
    from shapely.geometry import shape, mapping
    from shapely import wkt
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    print("警告: 未安装 Shapely 库。将使用简化的 GeoHash 中心点计算方法。")
    # 可以考虑在此处导入 regex 或其他库来更安全地解析 WKT，但为了简单起见，我们使用基本的字符串操作。

def calculate_centroid_simple(wkt_geom):
    """使用简单的字符串解析方法计算 WKT 多边形的近似中心点"""
    # 这是一个非常简化的实现，假设是 MULTIPOLYGON(((...))) 或 POLYGON((...))
    # 并且只取第一个环的第一个点作为参考来估算中心
    # 更精确的方法需要使用 GIS 库如 Shapely
    try:
        # 移除 MULTIPOLYGON 和括号，获取坐标部分
        # 例如: "MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2)))" -> "1 2, 3 4, 5 6, 1 2"
        import re
        # 匹配最内层的括号内容 (假设只有一个主多边形)
        match = re.search(r'\(\(([^)]+)\)\)', wkt_geom)
        if not match:
             # 尝试匹配 POLYGON
             match = re.search(r'\(([^)]+)\)', wkt_geom.split('POLYGON', 1)[-1])
        if not match:
            raise ValueError("无法从 WKT 提取坐标")

        coords_str = match.group(1)
        coords = []
        for pair in coords_str.split(','):
            lon_str, lat_str = pair.strip().split()
            coords.append((float(lon_str), float(lat_str)))

        if not coords:
            raise ValueError("未找到坐标")

        # 计算所有坐标的平均值作为近似中心点
        # 注意：这在跨经度180度或极地附近可能不准确，但对于一般情况足够
        avg_lon = sum(c[0] for c in coords) / len(coords)
        avg_lat = sum(c[1] for c in coords) / len(coords)
        return avg_lon, avg_lat
    except Exception as e:
        print(f"计算中心点时出错 (简化方法): {e}")
        # 回退：使用第一个坐标点
        try:
             match = re.search(r'(-?\d+\.?\d*)\s+(-?\d+\.?\d*)', wkt_geom)
             if match:
                 return float(match.group(1)), float(match.group(2))
        except:
             pass
        raise ValueError(f"无法计算 WKT 的中心点: {wkt_geom}")

def generate_osm_id_pure_code(wkt_geom, precision=12):
    """
    使用纯 Python 代码根据 WKT 生成一个 12 位整数 osm_id。
    """
    try:
        # 1. 计算几何的中心点 (需要解析 WKT)
        if SHAPELY_AVAILABLE:
            try:
                # 尝试使用 Shapely 解析 WKT 并获取精确中心点
                geom_obj = wkt.loads(wkt_geom)
                centroid = geom_obj.centroid
                lon, lat = centroid.x, centroid.y
            except Exception as e:
                print(f"Shapely 解析 WKT 失败: {e}, 回退到简化方法。")
                lon, lat = calculate_centroid_simple(wkt_geom)
        else:
            lon, lat = calculate_centroid_simple(wkt_geom)

        # 2. 使用中心点计算 GeoHash
        geohash_str = geohash2.encode(lat, lon, precision=10) # 10-12位通常足够区分

        # 3. 使用 hashlib (例如 SHA256) 对 GeoHash 进行哈希
        hash_object = hashlib.sha256(geohash_str.encode('utf-8'))
        hex_dig = hash_object.hexdigest()

        # 4. 将十六进制哈希转换为整数
        hash_int = int(hex_dig, 16)

        # 5. 取模以限制在 12 位整数范围内 (0 到 999,999,999,999)
        # 使用 10^12
        max_12_digit = 10**12
        osm_id = hash_int % max_12_digit

        # (可选) 确保 osm_id 不为 0，如果需要的话
        # if osm_id == 0:
        #     osm_id = 1

        return osm_id

    except Exception as e:
        print(f"生成 osm_id 时出错: {e}")
        # 可以选择抛出异常或返回一个默认/错误值
        # 这里选择返回一个可能冲突的 ID，但最好让调用者处理错误
        raise # 重新抛出异常，让上层处理


# ... (update_all_buildings_info_batch 和 process_building_batch 函数保持不变) ...


def insert_buildings_from_file(conn, file_path='buildings_output.txt'):
    """
    从文件中读取建筑物数据并插入到数据库
    文件格式: MULTIPOLYGON(((...)),高度
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        print(f"读取到 {len(lines)} 行数据")

        cur = conn.cursor()
        success_count = 0
        error_count = 0

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            try:
                # 解析行数据
                # 格式: MULTIPOLYGON(((...)),13.56
                parts = line.rsplit(',', 1)  # 从右边分割一次
                if len(parts) != 2:
                    print(f"✗ 第{line_num}行格式错误: {line}")
                    error_count += 1
                    continue

                wkt_geom_raw = parts[0].strip()
                height_str = parts[1].strip()

                # --- 处理潜在的引号问题 ---
                # 移除可能存在的首尾双引号或单引号
                wkt_geom = wkt_geom_raw.strip().strip('"').strip("'")
                # --- 处理结束 ---

                # --- 放宽或修改 WKT 格式验证 ---
                # 更宽松的检查（或者完全移除，让 PostGIS 报错）
                if not (wkt_geom.upper().startswith('MULTIPOLYGON') and wkt_geom.endswith('))')):
                     print(f"✗ 第{line_num}行WKT可能格式错误 (不以MULTIPOLYGON开头或不以))结尾): {wkt_geom}")
                     error_count += 1
                     continue
                # --- 修改结束 ---

                # 验证高度
                try:
                    building_height = float(height_str)
                except ValueError:
                    print(f"✗ 第{line_num}行高度格式错误: {height_str}")
                    error_count += 1
                    continue

                # --- 关键修改：在 Python 中生成 osm_id ---
                try:
                    osm_id = generate_osm_id_pure_code(wkt_geom)
                    print(f"  -> 为 WKT 生成的 osm_id: {osm_id}") # 可选：调试输出
                except Exception as gen_id_error:
                    print(f"✗ 第{line_num}行生成 osm_id 失败: {gen_id_error} - WKT: {wkt_geom}")
                    error_count += 1
                    continue # 跳过这一行
                # --- 修改结束 ---


                # 插入数据库
                # 修改 INSERT 语句，包含 osm_id，其值来自 Python 计算
                insert_query = """
                    INSERT INTO hz_yuhang_buildings (geom, building_height, osm_id)
                    VALUES (
                        ST_GeomFromText(%s, 4326),
                        %s,
                        %s -- 直接使用 Python 计算好的 osm_id
                    )
                """

                # 执行插入，传入 wkt_geom, building_height, osm_id
                cur.execute(insert_query, (wkt_geom, building_height, osm_id)) # 注意参数顺序
                print(f"✓ 第{line_num}行插入成功: 高度={building_height}米, osm_id={osm_id}")
                success_count += 1

                # 每100条提交一次，避免事务过大
                if (success_count + error_count) % 100 == 0:
                    try:
                        conn.commit()
                        print(f"已提交 {success_count + error_count} 条记录")
                    except psycopg2.Error as commit_error:
                         print(f"✗ 提交 {success_count + error_count} 条记录时出错: {commit_error}")
                         conn.rollback()
                         # 粗略估计失败数量，实际可能不同，这里简单处理
                         error_in_batch = 100 - (success_count % 100) if success_count % 100 != 0 else 0
                         error_count += error_in_batch
                         success_count -= (100 - error_in_batch)


            except psycopg2.Error as db_error: # 捕获数据库特定错误
                 print(f"✗ 第{line_num}行数据库插入错误: {db_error}")
                 # 打印出 WKT 可能有助于调试
                 print(f"    WKT: {wkt_geom}, osm_id: {osm_id}")
                 error_count += 1
                 conn.rollback() # 回滚当前事务
                 continue
            except Exception as e: # 捕获其他未预期的 Python 错误
                print(f"✗ 第{line_num}行处理错误: {str(e)}")
                error_count += 1
                # 不回滚整个事务，继续处理下一行
                continue # 这个 continue 在循环末尾是多余的，但显式写出更清晰

        # 提交剩余的更改
        try:
            # 只有在还有未提交的成功记录时才提交
            if (success_count + error_count) % 100 != 0 or success_count % 100 != 0:
                conn.commit()
                print(f"已提交剩余 {success_count + error_count - (success_count // 100 * 100)} 条记录")
        except psycopg2.Error as commit_error:
             print(f"✗ 提交剩余记录时出错: {commit_error}")
             conn.rollback()
             # 可能需要更精确地更新 error_count，这里简化处理
             # 假设剩余未提交的都失败了（这可能不准确）
             # remaining = (success_count + error_count) % 100
             # error_count += remaining
             # success_count -= remaining

        cur.close()

        print(f"\n文件数据插入完成!")
        print(f"成功插入: {success_count}")
        print(f"处理失败: {error_count}")
        print(f"总体成功率: {success_count / len(lines) * 100:.1f}%" if lines and len(lines) > 0 else "0%")

        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": len(lines),
            "success_rate": success_count / len(lines) * 100 if lines and len(lines) > 0 else 0
        }

    except FileNotFoundError:
        print(f"✗ 文件 {file_path} 不存在")
        return None
    except Exception as e:
        print(f"✗ 读取文件时出错: {str(e)}")
        # 尝试关闭游标（如果已打开）
        try:
            if 'cur' in locals() and cur and not cur.closed:
                cur.close()
        except:
            pass
        return None

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

        # 执行插入数据
        insert_result = insert_buildings_from_file(conn, 'buildings_output.txt')
        if insert_result:
             print("插入结果:", insert_result)

        # 关闭连接
        conn.close()

    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
