import os
import subprocess
import paramiko
import threading
import time
from config import node_num, abnormal_scenario, server_ip
from typing import List, Dict, Any

def startConfigNode(index):
    """启动指定索引的ConfigNode"""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[index], username="ubuntu", password="Dwf12345")
        
        stdin, stdout, stderr = ssh.exec_command(
            "sudo ./apache-iotdb-2.0.4-all-bin/sbin/start-confignode.sh -d", get_pty=True)
        print(f"启动 ConfigNode {index}")
        
        # 读取输出
        while not stdout.channel.exit_status_ready():
            result = stdout.readline()
            if result:
                print(f"{server_ip[index]} {result}", end="")
            if stdout.channel.exit_status_ready():
                remaining = stdout.readlines()
                for line in remaining:
                    print(f"{server_ip[index]} {line}", end="")
                break
                
        time.sleep(5)  # 给予ConfigNode启动时间
        
    except Exception as e:
        print(f"启动ConfigNode {index} 时出错: {e}")
    finally:
        if 'ssh' in locals():
            ssh.close()

def startDataNode(index):
    """启动指定索引的DataNode"""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[index], username="ubuntu", password="Dwf12345")
        
        stdin, stdout, stderr = ssh.exec_command(
            "sudo ./apache-iotdb-2.0.4-all-bin/sbin/start-datanode.sh -d", get_pty=True)
        print(f"启动 DataNode {index}")
        
        # 读取输出
        while not stdout.channel.exit_status_ready():
            result = stdout.readline()
            if result:
                print(f"{server_ip[index]} {result}", end="")
            if stdout.channel.exit_status_ready():
                remaining = stdout.readlines()
                for line in remaining:
                    print(f"{server_ip[index]} {line}", end="")
                break
                
    except Exception as e:
        print(f"启动DataNode {index} 时出错: {e}")
    finally:
        if 'ssh' in locals():
            ssh.close()

def stopNode(index, only_datanode=False):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[index], username="ubuntu", password="Dwf12345")
        if not only_datanode:
            stdin, stdout, stderr = ssh.exec_command(
                "sudo ./apache-iotdb-2.0.4-all-bin/sbin/stop-confignode.sh", get_pty=True)
            while not stdout.channel.exit_status_ready():
                result = stdout.readline()
                print(server_ip[index], result)
                if stdout.channel.exit_status_ready():
                    a = stdout.readlines()
                    break
            time.sleep(5)
        stdin, stdout, stderr = ssh.exec_command(
            "sudo ./apache-iotdb-2.0.4-all-bin/sbin/stop-datanode.sh", get_pty=True)
        while not stdout.channel.exit_status_ready():
            result = stdout.readline()
            print(server_ip[index], result)
            if stdout.channel.exit_status_ready():
                a = stdout.readlines()
                break
    except Exception as e:
        print(e)
    finally:
        ssh.close()

def monitor_and_restart(config_threads, data_threads, restart_count):
    """
    监控ConfigNode和DataNode的运行状态，当发现节点停止时自动重启
    
    参数:
        config_threads: ConfigNode线程列表
        data_threads: DataNode线程列表
        restart_count: 记录每个节点的重启次数
    """
    try:
        while True:
            time.sleep(1500)
            print("\n当前重启计数:", restart_count)
            
            # 检查并重启ConfigNode
            for i, t in enumerate(config_threads):
                if t.is_alive():
                    print(f"ConfigNode {i+1} 运行中")
                else:
                    print(f"ConfigNode {i+1} 已停止，正在重启...")
                    # 重启ConfigNode
                    new_thread = threading.Thread(target=startConfigNode, args=(i,))
                    new_thread.start()
                    config_threads[i] = new_thread
                    restart_count[i] += 1
            
            # 检查并重启DataNode
            for i, t in enumerate(data_threads):
                if t.is_alive():
                    print(f"DataNode {i+1} 运行中")
                else:
                    print(f"DataNode {i+1} 已停止，正在重启...")
                    # 重启DataNode
                    new_thread = threading.Thread(target=startDataNode, args=(i,))
                    new_thread.start()
                    data_threads[i] = new_thread
                    restart_count[i] += 1
                    
    except KeyboardInterrupt:
        print("\n收到中断信号，正在退出监控...")
    finally:
        # 等待所有线程结束
        print("等待所有节点线程结束...")
        for t in config_threads:
            t.join()
        for t in data_threads:
            t.join()
        print("所有节点线程已结束")

def parse_test_matrices(source_filename):
    """
    从源测试结果文件中读取最后一个Result Matrix和Latency (ms) Matrix的完整内容，
    并返回包含这两个矩阵数据的字典
    
    参数:
        source_filename: 源测试结果文件名（需要读取的文件）
    
    返回:
        dict: 包含两个矩阵数据的字典，格式为:
              {
                  'result_matrix': [矩阵行列表],
                  'latency_matrix': [矩阵行列表]
              }
              如果解析失败则返回None
    """
    try:
        # 1. 读取源文件所有内容，按行存储
        with open(source_filename, 'r', encoding='utf-8') as source_file:
            lines = [line.rstrip('\n') for line in source_file]  # 保留行结构，去除换行符
        
        # 2. 定位最后一个Result Matrix的起始和结束位置
        # 矩阵起始标志：以"-----------------------------------Result Matrix----------------------------------------------------------"开头
        # 矩阵结束标志：以"---------------------------------------------------------------------------------------------------------------------------------"开头
        result_matrix_start = -1
        result_matrix_end = -1
        # 从后向前查找，确保获取最后一个Result Matrix
        for i in range(len(lines)-1, -1, -1):
            line = lines[i]
            # 匹配Result Matrix起始行（允许前后有空格，兼容不同格式）
            if line.strip().startswith("----------------------------------------------------------Result Matrix"):
                result_matrix_start = i
                # 找到起始行后，向前查找对应的结束行
                for j in range(i, len(lines)):
                    if lines[j].strip().startswith("---------------------------------------------------------------------------------------"):
                        result_matrix_end = j
                        break
                break  # 找到最后一个矩阵后退出循环
        
        # 3. 定位最后一个Latency (ms) Matrix的起始和结束位置
        latency_matrix_start = -1
        latency_matrix_end = -1
        for i in range(len(lines)-1, -1, -1):
            line = lines[i]
            # 匹配Latency Matrix起始行
            if line.strip().startswith("--------------------------------------------------------------------------Latency (ms) Matrix"):
                latency_matrix_start = i
                # 找到起始行后，向前查找对应的结束行
                for j in range(i, len(lines)):
                    if lines[j].strip().startswith("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------"):
                        latency_matrix_end = j
                        break
                break
        
        # 4. 验证矩阵是否完整找到
        if result_matrix_start == -1 or result_matrix_end == -1:
            print(f"错误：在源文件 {source_filename} 中未找到完整的Result Matrix")
            return None
        if latency_matrix_start == -1 or latency_matrix_end == -1:
            print(f"错误：在源文件 {source_filename} 中未找到完整的Latency (ms) Matrix")
            return None
        
        # 5. 提取两个矩阵的完整内容（包含表头、数据行和分隔线）
        result_matrix_content = lines[result_matrix_start : result_matrix_end + 1]
        latency_matrix_content = lines[latency_matrix_start : latency_matrix_end + 1]
        
        # 6. 返回包含两个矩阵数据的字典
        results = {
            'result_matrix': result_matrix_content,
            'latency_matrix': latency_matrix_content
        }
        
        print(f"成功：已从 {source_filename} 中解析出矩阵数据")
        return results
    
    except FileNotFoundError:
        print(f"错误：源文件 {source_filename} 不存在")
        return None
    except PermissionError:
        print(f"错误：没有权限读取 {source_filename}")
        return None
    except Exception as e:
        print(f"解析文件时发生未知错误：{str(e)}")
        return None
    
def run_bat_and_parse(bat_path, result_file_path):
    """
    先执行bat文件，再解析结果文件并输出结果
    
    参数:
        bat_path: bat文件的路径
        result_file_path: 结果文件的路径
        save_path: 文件暂存的路径
    
    返回:
        解析得到的结果字典，如果有错误则返回None
    """
    try:
        timeout_seconds = 1500  # 设置超时时间为25分钟
        bat_directory = os.path.dirname(bat_path)
        # 获取bat文件的文件名
        bat_filename = os.path.basename(bat_path)
        print(f"开始执行bat文件: {bat_path}")
        
        # 执行bat文件
        print(f"将进入目录: {bat_directory}")
        print(f"将执行命令: .\\{bat_filename}")

        process = subprocess.Popen(
            bat_path, 
            cwd=bat_directory,   # 设置子进程的工作目录
            text=True            # 将输出视为文本
        )
        
        print(f"bat文件 '{bat_path}' 已启动，等待最多 {timeout_seconds} 秒...")

        # 等待子进程完成，设置超时
        return_code = process.wait(timeout=timeout_seconds)

        if return_code is not None:
            # 子进程在超时时间内完成
            print(f"bat文件执行完成，返回码: {return_code}")
        else:
            print(f"主程序继续")

    except subprocess.TimeoutExpired:
        # 如果 process.wait() 抛出 TimeoutExpired 异常，说明子进程在规定时间内没有完成
        print(f"主程序继续")
    except Exception as e:
        print(f"执行bat文件时发生未知错误: {e}")

    print("--- 60秒（或bat文件完成后）后，继续执行后续代码 ---")
        
    # 解析结果文件
    print(f"开始解析结果文件: {result_file_path}")
    results = parse_test_matrices(result_file_path)
    
    if not results:
        print("未能解析到有效结果")
        
    return results
    

def parse_result_matrix(matrix_lines: List[str]) -> Dict[str, Dict[str, float]]:
    """解析Result Matrix为结构化数据"""
    result = {}
    # 跳过第一行分隔线和第二行表头
    for line in matrix_lines[2:-1]:
        parts = line.strip().split()
        if len(parts) >= 6:
            operation = parts[0]
            result[operation] = {
                "okOperation": int(parts[1]),
                "okPoint": int(parts[2]),
                "failOperation": int(parts[3]),
                "failPoint": int(parts[4]),
                "throughput": float(parts[5])
            }
    return result

def parse_latency_matrix(matrix_lines: List[str]) -> Dict[str, Dict[str, float]]:
    """解析Latency Matrix为结构化数据"""
    result = {}
    # 跳过第一行分隔线和第二行表头
    for line in matrix_lines[2:-1]:
        parts = line.strip().split()
        if len(parts) >= 13:
            operation = parts[0]
            result[operation] = {
                "AVG": float(parts[1]),
                "MIN": float(parts[2]),
                "P10": float(parts[3]),
                "P25": float(parts[4]),
                "MEDIAN": float(parts[5]),
                "P75": float(parts[6]),
                "P90": float(parts[7]),
                "P95": float(parts[8]),
                "P99": float(parts[9]),
                "P999": float(parts[10]),
                "MAX": float(parts[11]),
                "SLOWEST_THREAD": float(parts[12])
            }
    return result

def calculate_phase_averages(phase_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """计算同一测试阶段多次实验的平均值"""
    if not phase_results or len(phase_results) == 0:
        return None
        
    # 从第一个结果的result_matrix中提取操作名称
    # result_matrix是字符串列表，格式如下：
    # [分隔线, 表头, 操作1数据, 操作2数据, ..., 结束分隔线]
    first_result_matrix = phase_results[0]["result_matrix"]
    
    # 提取操作名称（跳过分隔线和表头，取到结束分隔线前）
    operations = []
    for line in first_result_matrix[2:-1]:  # 索引2是第一行数据，-1是结束分隔线
        # 每行第一个单词是操作名称
        op_name = line.strip().split()[0]
        operations.append(op_name)
    
    # 初始化平均值存储结构
    avg_result = {
        "result_matrix": {op: {} for op in operations},
        "latency_matrix": {op: {} for op in operations}
    }
    
    # 解析Result Matrix的表头，确定各列名称
    result_header = first_result_matrix[1].strip().split()
    # 应该是：["Operation", "okOperation", "okPoint", "failOperation", "failPoint", "throughput(point/s)"]
    
    # 计算Result Matrix平均值
    for op in operations:
        sums = {
            "okOperation": 0,
            "okPoint": 0,
            "failOperation": 0,
            "failPoint": 0,
            "throughput": 0
        }
        
        for test in phase_results:
            # 找到当前操作在测试结果中的行
            for line in test["result_matrix"][2:-1]:  # 跳过分隔线和表头
                if line.strip().startswith(op):
                    parts = line.strip().split()
                    if len(parts) >= 6:
                        sums["okOperation"] += int(parts[1])
                        sums["okPoint"] += int(parts[2])
                        sums["failOperation"] += int(parts[3])
                        sums["failPoint"] += int(parts[4])
                        sums["throughput"] += float(parts[5])
                    break  # 找到后退出循环
        
        count = len(phase_results)
        for key, value in sums.items():
            avg_result["result_matrix"][op][key] = value / count
    
    # 解析Latency Matrix的表头
    first_latency_matrix = phase_results[0]["latency_matrix"]
    latency_header = first_latency_matrix[1].strip().split()
    # 应该是：["Operation", "AVG", "MIN", "P10", ..., "SLOWEST_THREAD"]
    
    # 计算Latency Matrix平均值
    for op in operations:
        sums = {
            "AVG": 0, "MIN": 0, "P10": 0, "P25": 0, "MEDIAN": 0, "P75": 0,
            "P90": 0, "P95": 0, "P99": 0, "P999": 0, "MAX": 0, "SLOWEST_THREAD": 0
        }
        
        for test in phase_results:
            # 找到当前操作在测试结果中的行
            for line in test["latency_matrix"][2:-1]:  # 跳过分隔线和表头
                if line.strip().startswith(op):
                    parts = line.strip().split()
                    if len(parts) >= 13:
                        sums["AVG"] += float(parts[1])
                        sums["MIN"] += float(parts[2])
                        sums["P10"] += float(parts[3])
                        sums["P25"] += float(parts[4])
                        sums["MEDIAN"] += float(parts[5])
                        sums["P75"] += float(parts[6])
                        sums["P90"] += float(parts[7])
                        sums["P95"] += float(parts[8])
                        sums["P99"] += float(parts[9])
                        sums["P999"] += float(parts[10])
                        sums["MAX"] += float(parts[11])
                        sums["SLOWEST_THREAD"] += float(parts[12])
                    break  # 找到后退出循环
        
        count = len(phase_results)
        for key, value in sums.items():
            avg_result["latency_matrix"][op][key] = value / count
    
    return avg_result

def format_matrix_for_output(avg_data: Dict[str, Any], matrix_type: str) -> List[str]:
    """将平均数据格式化为原始矩阵样式的输出"""
    matrix_data = avg_data[matrix_type]
    lines = []
    
    # 添加矩阵标题分隔线
    if matrix_type == "result_matrix":
        lines.append("-----------------------------------Result Matrix----------------------------------------------------------")
        lines.append("Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      ")
    else:  # latency_matrix
        lines.append("--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------")
        lines.append("Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD")
    
    # 添加操作数据行
    for op in matrix_data:
        data = matrix_data[op]
        if matrix_type == "result_matrix":
            line = (f"{op:<24}{int(round(data['okOperation'])):<24}{int(round(data['okPoint'])):<24}"
                   f"{int(round(data['failOperation'])):<24}{int(round(data['failPoint'])):<24}{data['throughput']:.2f}")
        else:
            line = (f"{op:<24}{data['AVG']:.2f}{data['MIN']:>10.2f}{data['P10']:>10.2f}{data['P25']:>10.2f}"
                   f"{data['MEDIAN']:>10.2f}{data['P75']:>10.2f}{data['P90']:>10.2f}{data['P95']:>10.2f}"
                   f"{data['P99']:>10.2f}{data['P999']:>10.2f}{data['MAX']:>10.2f}{data['SLOWEST_THREAD']:>15.2f}")
        lines.append(line)
    
    # 添加结束分隔线
    if matrix_type == "result_matrix":
        lines.append("---------------------------------------------------------------------------------------------------------------------------------")
    else:
        lines.append("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    
    return lines