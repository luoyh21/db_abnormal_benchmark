import json
import time
import threading
import logging
from config import node_num, server_ip, abnormal_scenario, OUTPUT_STORE_PATH
from typing import List
import os
import paramiko
from tools import startConfigNode, startDataNode, stopNode, run_bat_and_parse, start_monitoring_system

# 配置日志
os.makedirs(OUTPUT_STORE_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_STORE_PATH, 'info.log'), encoding='utf-8'),
        logging.StreamHandler()  # 同时输出到控制台
    ]
)


def create_asymmetric_network_partition_groups(node_count: int) -> tuple:
    """
    创建非对称式网络分区的节点分组
    
    参数:
        node_count: 节点总数（必须>=3）
    
    返回:
        tuple: (group1, group2, bridge_nodes) 
               group1包含0号节点，group2包含最后一个节点，bridge_nodes为保持连接的节点对
    """
    if node_count < 3:
        raise ValueError("节点数量必须>=3才能进行非对称式网络分区")
    
    # 计算分组大小，尽量平均分配
    mid_point = node_count // 2
    
    # group1包含0号节点和前半部分节点
    group1 = list(range(mid_point))
    # group2包含最后一个节点和后半部分节点
    group2 = list(range(mid_point, node_count))
    
    # 确保0号节点在group1，最后一个节点在group2
    if 0 not in group1:
        group1.append(0)
    if (node_count - 1) not in group2:
        group2.append(node_count - 1)
    
    # 桥接节点：0号节点和最后一个节点保持连接
    bridge_nodes = (0, node_count - 1)
    
    logging.info(f"非对称式网络分区分组：")
    logging.info(f"  Group1: {group1} (包含0号节点)")
    logging.info(f"  Group2: {group2} (包含{node_count-1}号节点)")
    logging.info(f"  桥接连接: 节点{bridge_nodes[0]} <-> 节点{bridge_nodes[1]} (保持连接)")
    
    return group1, group2, bridge_nodes


def apply_asymmetric_network_partition(group1: List[int], group2: List[int], bridge_nodes: tuple):
    """
    应用非对称式网络分区：使用iptables阻断两组节点间的通信，但保持桥接节点间的连接
    
    参数:
        group1: 第一组节点索引
        group2: 第二组节点索引  
        bridge_nodes: 桥接节点对 (node1, node2)
    """
    logging.info("\n【开始应用非对称式网络分区】")
    
    def block_communication_except_bridge(from_nodes: List[int], to_nodes: List[int], bridge_pair: tuple):
        """阻断from_nodes到to_nodes的通信，但保留桥接节点间的连接"""
        threads = []
        bridge_node1, bridge_node2 = bridge_pair
        
        for from_idx in from_nodes:
            for to_idx in to_nodes:
                if from_idx != to_idx:
                    # 如果是桥接节点间的通信，跳过不阻断
                    if (from_idx == bridge_node1 and to_idx == bridge_node2) or \
                       (from_idx == bridge_node2 and to_idx == bridge_node1):
                        logging.info(f"保持桥接连接：节点{from_idx} <-> 节点{to_idx}")
                        continue
                    
                    t = threading.Thread(
                        target=_block_node_communication,
                        args=(from_idx, server_ip[to_idx])
                    )
                    threads.append(t)
                    t.start()
        
        # 等待所有阻断操作完成
        for t in threads:
            t.join()
    
    # 双向阻断通信（除了桥接连接）
    logging.info(f"阻断 Group1 {group1} 到 Group2 {group2} 的通信（保留桥接连接）...")
    block_communication_except_bridge(group1, group2, bridge_nodes)
    
    logging.info(f"阻断 Group2 {group2} 到 Group1 {group1} 的通信（保留桥接连接）...")
    block_communication_except_bridge(group2, group1, bridge_nodes)
    
    logging.info("【非对称式网络分区应用完成】两组节点间通信已部分阻断，桥接连接保持")


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
        
        logging.info(f"节点 {node_idx} ({server_ip[node_idx]}) 已阻断到 {target_ip} 的通信")
        
    except Exception as e:
        logging.error(f"节点 {node_idx} 阻断通信时出错: {e}")
    finally:
        if 'ssh' in locals():
            ssh.close()


def restore_network_connectivity():
    """
    恢复网络连接：清空所有节点的iptables规则
    """
    logging.info("\n【开始恢复网络连接】")
    
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
            
            logging.info(f"节点 {node_idx} ({server_ip[node_idx]}) 的iptables规则已清空")
            
        except Exception as e:
            logging.error(f"节点 {node_idx} 恢复网络连接时出错: {e}")
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
    
    logging.info("【网络连接恢复完成】所有节点的iptables规则已清空")


def asymmetric_network_partition_scenario(bat_path: str = "test.bat", 
                                        test_result_file_path: str = "test_result.txt",
                                        storing_path: str = "single_run_results") -> None:
    """
    运行单次非对称式网络分区场景
    
    参数:
        bat_path: 测试脚本路径
        test_result_file_path: 单次测试结果文件路径
        storing_path: 结果输出路径
    """
    current_time = int(time.time())
    output_store_path = f"{storing_path}\\result_{abnormal_scenario}_{current_time}\\single_run.json"
    
    logging.info(f"\n{'='*80}")
    logging.info(f"开始单次非对称式网络分区场景实验")
    logging.info(f"{'='*80}")
    
    # 调用非对称式网络分区场景函数
    exp_result = asymmetric_network_partition_single_run(
        bat_path=bat_path,
        test_result_file_path=test_result_file_path,
        output_store_path=output_store_path
    )
    
    logging.info(f"\n实验完成！结果已保存到 {output_store_path}")
    return exp_result


def asymmetric_network_partition_single_run(bat_path, test_result_file_path, output_store_path):
    """
    单次非对称式网络分区场景主函数：清理→启动→等待20分钟→第一次测试→第二次测试(期间进行网络分区)→结果存储→停止系统
    
    参数：
        bat_path: str - 测试用bat文件的完整路径
        test_result_file_path: str - 单次测试结果文件的完整路径
        output_store_path: str - 最终测试结果集合的存储路径
    
    返回：
        dict - 两次测试的结果集合（含状态信息）
    """
    # 初始化两次测试的结果集合
    all_test_results = {
        "scenario_name": "asymmetric_network_partition_single_run",
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "node_count": node_num,
        "server_ips": server_ip,
        "test_results": [],
        "end_time": "",
        "status": "running"
    }

    try:
        # 验证节点数量>=3
        if node_num < 3:
            raise ValueError(f"节点数量 {node_num} 小于3，无法进行非对称式网络分区")
        
        # 创建节点分组
        group1, group2, bridge_nodes = create_asymmetric_network_partition_groups(node_num)
        all_test_results["group1"] = group1
        all_test_results["group2"] = group2
        all_test_results["bridge_nodes"] = bridge_nodes

        # -------------------------- 1. 清理所有节点 --------------------------
        logging.info("【步骤1/6】清理所有节点...")
        clean_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            clean_threads.append(t)

        time.sleep(10)
        logging.info("【步骤1/6】所有节点清理完成")

        # 同时清空所有节点的iptables规则（预防性清理）
        logging.info("【步骤1/6】预防性清理iptables规则...")
        restore_network_connectivity()

        # -------------------------- 2. 启动所有ConfigNode --------------------------
        logging.info("\n【步骤2/6】启动所有ConfigNode...")
        config_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startConfigNode, args=(idx,))
            t.start()
            config_threads.append(t)
        time.sleep(60)
        logging.info("【步骤2/6】所有ConfigNode启动完成")

        # -------------------------- 3. 启动所有DataNode --------------------------
        logging.info("\n【步骤3/6】启动所有DataNode...")
        data_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startDataNode, args=(idx,))
            t.start()
            data_threads.append(t)
        time.sleep(60)
        logging.info("【步骤3/6】所有DataNode启动完成")

        # -------------------------- 4. 启动节点监控系统 --------------------------
        logging.info("\n【步骤4/6】启动节点监控系统（Prometheus + Grafana）...")
        start_monitoring_system()
        logging.info("【步骤4/6】节点监控系统启动完成")

        # -------------------------- 5. 第一次测试：等待20分钟后进行 --------------------------
        logging.info("\n【步骤5/6】等待20分钟，准备第一次测试（节点启动后稳定测试）...")
        time.sleep(20 * 60)  # 等待20分钟
        first_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        first_test["test_phase"] = "normal"
        first_test["phase_description"] = "节点启动后稳定测试（正常状态）"
        all_test_results["test_results"].append(first_test)
        logging.info("【步骤5/6】第一次测试完成")

        # -------------------------- 6. 第二次测试：等待15分钟后开始，期间进行网络分区操作 --------------------------
        logging.info("\n【步骤6/6】等待15分钟后开始第二次测试（期间进行非对称式网络分区操作）...")
        time.sleep(15 * 60)  # 等待15分钟
        
        # 创建异步执行网络分区操作的线程
        def network_partition_operation():
            logging.info("等待5分钟后应用非对称式网络分区...")
            time.sleep(5 * 60)  # 等待5分钟
            
            logging.info("开始应用非对称式网络分区...")
            apply_asymmetric_network_partition(group1, group2, bridge_nodes)
            
            logging.info("等待5分钟后恢复网络连接...")
            time.sleep(5 * 60)  # 等待5分钟
            
            logging.info("开始恢复网络连接...")
            restore_network_connectivity()
            logging.info("非对称式网络分区操作完成")
        
        # 启动网络分区操作线程
        operation_thread = threading.Thread(target=network_partition_operation)
        operation_thread.start()
        
        # 同时开始第二次测试
        logging.info("开始第二次测试...")
        second_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        second_test["test_phase"] = "abnormal"
        second_test["phase_description"] = f"非对称式网络分区测试（异常状态 - Group1:{group1} vs Group2:{group2}，桥接:{bridge_nodes}）"
        second_test["partition_groups"] = {"group1": group1, "group2": group2}
        second_test["bridge_nodes"] = bridge_nodes
        all_test_results["test_results"].append(second_test)
        
        # 等待网络分区操作完成
        operation_thread.join()
        logging.info("【步骤6/6】第二次测试和非对称式网络分区操作均完成")

        # -------------------------- 7. 更新场景状态，存储结果 --------------------------
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "finished"
        logging.info(f"\n{'='*60}")
        logging.info(f"场景执行完成！开始将结果写入存储文件：{output_store_path}")
        
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ 结果已成功存储到 {output_store_path}")

    except Exception as e:
        error_msg = f"场景执行异常：{str(e)}"
        logging.error(f"\n❌ {error_msg}")
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "failed"
        all_test_results["error_msg"] = error_msg
        
        # 异常情况下也要恢复网络连接
        try:
            restore_network_connectivity()
        except Exception as restore_error:
            logging.warning(f"⚠️ 恢复网络连接时出错: {restore_error}")
        
        # 存储异常状态下的结果
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        logging.warning(f"⚠️  已将异常状态下的结果存储到 {output_store_path}")

    finally:
        # 最终清理：确保网络连接恢复并停止所有节点
        try:
            logging.info("\n【最终步骤】确保网络连接恢复...")
            restore_network_connectivity()
        except Exception as e:
            logging.warning(f"⚠️ 最终网络恢复时出错: {e}")
        
        logging.info("【最终步骤】停止所有节点...")
        stop_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            stop_threads.append(t)
        time.sleep(10)
        logging.info("【最终步骤】所有节点停止完成")

    return all_test_results


if __name__ == "__main__":
    asymmetric_network_partition_scenario()
