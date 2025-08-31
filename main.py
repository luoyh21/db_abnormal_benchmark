import random
import subprocess
import paramiko
import threading
import time
from config import node_num, abnormal_scenario, server_ip
from node_outage import node_outage_scenario

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
                "sudo ./apache-iotdb-2.0.4-all-bin/sbin/stop-confignode.sh -d", get_pty=True)
            while not stdout.channel.exit_status_ready():
                result = stdout.readline()
                print(server_ip[index], result)
                if stdout.channel.exit_status_ready():
                    a = stdout.readlines()
                    break
            time.sleep(5)
        stdin, stdout, stderr = ssh.exec_command(
            "sudo ./apache-iotdb-2.0.4-all-bin/sbin/stop-datanode.sh -d", get_pty=True)
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
            time.sleep(10)
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

def parse_test_results(filename):
    """
    从文件底部向上解析测试结果，提取最后出现的INGESTION操作的吞吐率和成功率
    
    参数:
        filename: 测试结果文件名
    
    返回:
        包含吞吐率和成功率的字典，如果解析失败则返回None
    """
    try:
        with open(filename, 'r') as file:
            # 读取所有行并反转，从底部开始查找
            lines = file.readlines()
            reversed_lines = list(reversed(lines))
            
            # 从底部向上查找INGESTION行
            ingestion_line = None
            for line in reversed_lines:
                stripped_line = line.strip()
                if stripped_line.startswith("INGESTION"):
                    ingestion_line = stripped_line
                    break
            
            if ingestion_line is None:
                print("未找到INGESTION操作的结果")
                return None
            
            # 解析INGESTION行的数据
            parts = ingestion_line.split()
            if len(parts) < 6:
                print("INGESTION行格式不正确")
                return None
            
            # 提取所需数据
            ok_operations = int(parts[1])
            total_operations = ok_operations + int(parts[3])
            throughput = float(parts[5])
            
            # 计算成功率
            success_rate = (ok_operations / total_operations) * 100 if total_operations > 0 else 0
            
            return {
                "throughput": throughput,
                "success_rate": success_rate
            }
            
    except FileNotFoundError:
        print(f"文件 {filename} 不存在")
        return None
    except Exception as e:
        print(f"解析文件时发生错误: {str(e)}")
        return None
    
def run_bat_and_parse(bat_path, result_file_path):
    """
    先执行bat文件，再解析结果文件并输出结果
    
    参数:
        bat_path: bat文件的路径
        result_file_path: 结果文件的路径
    
    返回:
        解析得到的结果字典，如果有错误则返回None
    """
    try:
        print(f"开始执行bat文件: {bat_path}")
        
        # 执行bat文件
        result = subprocess.run(
            bat_path,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print(f"bat文件执行完成，返回码: {result.returncode}")
        
        # 等待结果文件生成（如果需要）
        # 这里可以根据实际情况调整等待时间
        time.sleep(20)
        
        # 解析结果文件
        print(f"开始解析结果文件: {result_file_path}")
        results = parse_test_results(result_file_path)
        
        if results:
            print("\n解析结果:")
            print(f"吞吐率: {results['throughput']} points/s")
            print(f"成功率: {results['success_rate']:.2f}%")
        else:
            print("未能解析到有效结果")
            
        return results
        
    except subprocess.CalledProcessError as e:
        print(f"bat文件执行失败: {e.stderr}")
        return None
    except Exception as e:
        print(f"执行过程中发生错误: {str(e)}")
        return None

if __name__ == "__main__":
    stop_event = threading.Event()  # 保留原stop_event，供监控函数（如monitor_and_restart）使用
    print(f"{'='*60}")
    print(f"启动程序 | 异常场景：{abnormal_scenario} | 节点数量：{node_num}")
    print(f"{'='*60}")

    if abnormal_scenario == "node_outage":
        # 执行节点宕机场景，自动触发三次测试并输出结果
        print("开始执行节点宕机测试流程...")
        node_outage_scenario()
    else:
        # 默认场景：仅启动所有节点，不执行测试
        print("\nℹ️  无异常场景（或场景配置错误），仅启动所有节点...")
        start_threads = []
        for i in range(node_num):
            # 先启动ConfigNode，再启动DataNode（确保依赖顺序）
            t_config = threading.Thread(target=startConfigNode, args=(i,))
            t_data = threading.Thread(target=startDataNode, args=(i,))
            start_threads.extend([t_config, t_data])
            t_config.start()
            time.sleep(2)  # 给ConfigNode启动缓冲时间
            t_data.start()
        
        # 等待所有启动线程完成
        for t in start_threads:
            t.join()
        print("\n✅ 所有节点（ConfigNode + DataNode）启动完成，程序保持运行...")
        
        config_threads = []
        data_threads = []
        restart_count = [0] * node_num
        monitor_thread = threading.Thread(
            target=monitor_and_restart,
            args=(config_threads, data_threads, restart_count),
            daemon=True
        )
        monitor_thread.start()
        monitor_thread.join()
            