import random

def modify_last_column(input_filename, output_filename):
    """
    读取输入文件，将每行的最后一列替换为0-50之间的随机数，并写入输出文件。

    Args:
        input_filename (str): 输入文件的路径。
        output_filename (str): 输出文件的路径。
    """
    try:
        with open(input_filename, 'r', encoding='utf-8') as infile, \
             open(output_filename, 'w', encoding='utf-8') as outfile:

            for line_num, line in enumerate(infile, 1):
                # 去除行尾的换行符
                line = line.rstrip('\n')

                if not line: # 跳过空行
                    outfile.write('\n')
                    continue

                # 按逗号分割字段
                parts = line.split(',')

                if len(parts) >= 4: # 确保至少有4列（经度，纬度，高度/值，最后一列）
                    # 生成 0 到 50 之间的随机浮点数 (保留两位小数)
                    # 你也可以使用 random.randint(0, 50) 来生成整数
                    new_last_value = round(random.uniform(0, 50), 2)

                    # 替换最后一列的值
                    parts[-1] = str(new_last_value)

                    # 将修改后的字段用逗号连接起来
                    modified_line = ','.join(parts)

                    # 写入输出文件
                    outfile.write(modified_line + '\n')
                else:
                    print(f"警告：第 {line_num} 行列数不足，已跳过: {line}")
                    # 也可以选择将原始行写入输出文件
                    # outfile.write(line + '\n')

        print(f"处理完成。修改后的内容已保存到 '{output_filename}'")

    except FileNotFoundError:
        print(f"错误：找不到文件 '{input_filename}'")
    except Exception as e:
        print(f"处理文件时发生错误: {e}")

if __name__ == "__main__":
    # --- 请根据你的实际情况修改以下文件名 ---
    input_file = "all_hit_points.csv"  # 你的原始文件名
    output_file = "modified_data.csv"          # 你希望保存的输出文件名
    # ------------------------------------------

    modify_last_column(input_file, output_file)