import json
import time
import threading
from config import node_num, server_ip, abnormal_scenario
from typing import List, Dict, Any
import os
import paramiko
from tools import startConfigNode, startDataNode, stopNode, run_bat_and_parse, start_monitoring_system


def create_network_partition_groups(node_count: int) -> tuple:
    """
    创建对称式网络分区的节点分组
    
    参数:
        node_count: 节点总数（必须为奇数）
    
    返回:
        tuple: (group1, group2) 其中group1包含0号节点且数量较多
    """
    if node_count % 2 == 0:
        raise ValueError("节点数量必须为奇数")
    
    # 计算分组大小
    larger_group_size = (node_count + 1) // 2
    
    # group1包含0号节点，数量较多
    group1 = list(range(larger_group_size))
    group2 = list(range(larger_group_size, node_count))
    
    print(f"网络分区分组：")
    print(f"  Group1 (较大组): {group1}")
    print(f"  Group2 (较小组): {group2}")
    
    return group1, group2


def apply_network_partition(group1: List[int], group2: List[int]):
    """
    应用网络分区：使用iptables阻断两组节点间的通信
    
    参数:
        group1: 第一组节点索引
        group2: 第二组节点索引
    """
    print("\n【开始应用网络分区】")
    
    def block_communication(from_nodes: List[int], to_nodes: List[int]):
        """阻断from_nodes到to_nodes的通信"""
        threads = []
        for from_idx in from_nodes:
            for to_idx in to_nodes:
                if from_idx != to_idx:
                    t = threading.Thread(
                        target=_block_node_communication,
                        args=(from_idx, server_ip[to_idx])
                    )
                    threads.append(t)
                    t.start()
        
        # 等待所有阻断操作完成
        for t in threads:
            t.join()
    
    # 双向阻断通信
    print(f"阻断 Group1 {group1} 到 Group2 {group2} 的通信...")
    block_communication(group1, group2)
    
    print(f"阻断 Group2 {group2} 到 Group1 {group1} 的通信...")
    block_communication(group2, group1)
    
    print("【网络分区应用完成】两组节点间通信已完全阻断")


def _block_node_communication(node_idx: int, target_ip: str):
    """
    在指定节点上阻断到目标IP的通信
    
    参数:
        node_idx: 源节点索引
        target_ip: 目标节点IP
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[node_idx], username="ubuntu", password="Dwf12345")
        
        # 使用iptables阻断到目标IP的通信
        block_command = f"sudo iptables -A OUTPUT -d {target_ip} -j DROP"
        stdin, stdout, stderr = ssh.exec_command(block_command, get_pty=True)
        
        # 等待命令执行完成
        stdout.channel.recv_exit_status()
        
        print(f"节点 {node_idx} ({server_ip[node_idx]}) 已阻断到 {target_ip} 的通信")
        
    except Exception as e:
        print(f"节点 {node_idx} 阻断通信时出错: {e}")
    finally:
        if 'ssh' in locals():
            ssh.close()


def restore_network_connectivity():
    """
    恢复网络连接：清空所有节点的iptables规则
    """
    print("\n【开始恢复网络连接】")
    
    def clear_node_iptables(node_idx: int):
        """清空指定节点的iptables规则"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(server_ip[node_idx], username="ubuntu", password="Dwf12345")
            
            # 清空iptables OUTPUT链的规则
            clear_command = "sudo iptables -F OUTPUT"
            stdin, stdout, stderr = ssh.exec_command(clear_command, get_pty=True)
            
            # 等待命令执行完成
            stdout.channel.recv_exit_status()
            
            print(f"节点 {node_idx} ({server_ip[node_idx]}) 的iptables规则已清空")
            
        except Exception as e:
            print(f"节点 {node_idx} 恢复网络连接时出错: {e}")
        finally:
            if 'ssh' in locals():
                ssh.close()
    
    # 并行恢复所有节点的网络连接
    threads = []
    for idx in range(node_num):
        t = threading.Thread(target=clear_node_iptables, args=(idx,))
        threads.append(t)
        t.start()
    
    # 等待所有恢复操作完成
    for t in threads:
        t.join()
    
    print("【网络连接恢复完成】所有节点的iptables规则已清空")


def symmetric_network_partition_scenario(bat_path: str = "test.bat", 
                                        test_result_file_path: str = "test_result.txt",
                                        storing_path: str = "single_run_results") -> None:
    """
    运行单次对称式网络分区场景
    
    参数:
        bat_path: 测试脚本路径
        test_result_file_path: 单次测试结果文件路径
        storing_path: 结果输出路径
    """
    current_time = int(time.time())
    output_store_path = f"{storing_path}\\result_{abnormal_scenario}_{current_time}\\single_run.json"
    
    print(f"\n{'='*80}")
    print(f"开始单次对称式网络分区场景实验")
    print(f"{'='*80}")
    
    # 调用对称式网络分区场景函数
    exp_result = symmetric_network_partition_single_run(
        bat_path=bat_path,
        test_result_file_path=test_result_file_path,
        output_store_path=output_store_path
    )
    
    print(f"\n实验完成！结果已保存到 {output_store_path}")
    return exp_result


def symmetric_network_partition_single_run(bat_path, test_result_file_path, output_store_path):
    """
    单次对称式网络分区场景主函数：清理→启动→等待20分钟→第一次测试→第二次测试(期间进行网络分区)→结果存储→停止系统
    
    参数：
        bat_path: str - 测试用bat文件的完整路径
        test_result_file_path: str - 单次测试结果文件的完整路径
        output_store_path: str - 最终测试结果集合的存储路径
    
    返回：
        dict - 两次测试的结果集合（含状态信息）
    """
    # 初始化两次测试的结果集合
    all_test_results = {
        "scenario_name": "symmetric_network_partition_single_run",
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "node_count": node_num,
        "server_ips": server_ip,
        "test_results": [],
        "end_time": "",
        "status": "running"
    }

    try:
        # 验证节点数量为奇数
        if node_num % 2 == 0:
            raise ValueError(f"节点数量 {node_num} 不是奇数，无法进行对称式网络分区")
        
        # 创建节点分组
        group1, group2 = create_network_partition_groups(node_num)
        all_test_results["group1"] = group1
        all_test_results["group2"] = group2

        # -------------------------- 1. 清理所有节点 --------------------------
        print("【步骤1/6】清理所有节点...")
        clean_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            clean_threads.append(t)

        time.sleep(10)
        print("【步骤1/6】所有节点清理完成")

        # 同时清空所有节点的iptables规则（预防性清理）
        print("【步骤1/6】预防性清理iptables规则...")
        restore_network_connectivity()

        # -------------------------- 2. 启动所有ConfigNode --------------------------
        print("\n【步骤2/6】启动所有ConfigNode...")
        config_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startConfigNode, args=(idx,))
            t.start()
            config_threads.append(t)
        time.sleep(60)
        print("【步骤2/6】所有ConfigNode启动完成")

        # -------------------------- 3. 启动所有DataNode --------------------------
        print("\n【步骤3/6】启动所有DataNode...")
        data_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startDataNode, args=(idx,))
            t.start()
            data_threads.append(t)
        time.sleep(60)
        print("【步骤3/6】所有DataNode启动完成")

        # -------------------------- 4. 启动节点监控系统 --------------------------
        print("\n【步骤4/6】启动节点监控系统（Prometheus + Grafana）...")
        start_monitoring_system()
        print("【步骤4/6】节点监控系统启动完成")

        # -------------------------- 5. 第一次测试：等待20分钟后进行 --------------------------
        print("\n【步骤5/6】等待20分钟，准备第一次测试（节点启动后稳定测试）...")
        time.sleep(20 * 60)  # 等待20分钟
        first_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        first_test["test_phase"] = "normal"
        first_test["phase_description"] = "节点启动后稳定测试（正常状态）"
        all_test_results["test_results"].append(first_test)
        print("【步骤5/6】第一次测试完成")

        # -------------------------- 6. 第二次测试：运行30分钟，期间进行网络分区操作 --------------------------
        print("\n【步骤6/6】开始第二次测试（运行30分钟，期间进行网络分区操作）...")
        
        # 创建异步执行网络分区操作的线程
        def network_partition_operation():
            print("等待10分钟后应用网络分区...")
            time.sleep(10 * 60)  # 等待10分钟
            
            print("开始应用网络分区...")
            apply_network_partition(group1, group2)
            
            print("等待5分钟后恢复网络连接...")
            time.sleep(5 * 60)  # 等待5分钟
            
            print("开始恢复网络连接...")
            restore_network_connectivity()
            print("网络分区操作完成")
        
        # 启动网络分区操作线程
        operation_thread = threading.Thread(target=network_partition_operation)
        operation_thread.start()
        
        # 同时开始第二次测试
        print("开始第二次测试...")
        second_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        second_test["test_phase"] = "abnormal"
        second_test["phase_description"] = f"对称式网络分区测试（异常状态 - Group1:{group1} vs Group2:{group2}）"
        second_test["partition_groups"] = {"group1": group1, "group2": group2}
        all_test_results["test_results"].append(second_test)
        
        # 等待网络分区操作完成
        operation_thread.join()
        print("【步骤6/6】第二次测试和网络分区操作均完成")

        # -------------------------- 7. 更新场景状态，存储结果 --------------------------
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "finished"
        print(f"\n{'='*60}")
        print(f"场景执行完成！开始将结果写入存储文件：{output_store_path}")
        
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        print(f"✅ 结果已成功存储到 {output_store_path}")

    except Exception as e:
        error_msg = f"场景执行异常：{str(e)}"
        print(f"\n❌ {error_msg}")
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "failed"
        all_test_results["error_msg"] = error_msg
        
        # 异常情况下也要恢复网络连接
        try:
            restore_network_connectivity()
        except Exception as restore_error:
            print(f"⚠️ 恢复网络连接时出错: {restore_error}")
        
        # 存储异常状态下的结果
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        print(f"⚠️  已将异常状态下的结果存储到 {output_store_path}")

    finally:
        # 最终清理：确保网络连接恢复并停止所有节点
        try:
            print("\n【最终步骤】确保网络连接恢复...")
            restore_network_connectivity()
        except Exception as e:
            print(f"⚠️ 最终网络恢复时出错: {e}")
        
        print("【最终步骤】停止所有节点...")
        stop_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            stop_threads.append(t)
        time.sleep(10)
        print("【最终步骤】所有节点停止完成")

    return all_test_results


if __name__ == "__main__":
    symmetric_network_partition_scenario()