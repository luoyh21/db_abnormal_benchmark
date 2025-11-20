import json
import time
import threading
import logging
import random
from config import node_num, server_ip, abnormal_scenario, OUTPUT_STORE_PATH, TRANSMISSION_DELAY_MS, DELAY_VARIANCE_MS
from typing import List
import os
import paramiko
from tools import startConfigNode, startDataNode, stopNode, run_bat_and_parse, start_monitoring_system, modify_db_switch

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


def get_network_interface(node_idx: int) -> str:
    """
    获取指定节点的网络接口名称
    
    参数:
        node_idx: 节点索引
    
    返回:
        网络接口名称（如eth0, ens3等）
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[node_idx], username="ubuntu", password="Dwf12345")
        
        # 获取默认路由的网络接口
        stdin, stdout, stderr = ssh.exec_command("ip route get 8.8.8.8 | awk '{for(i=1;i<=NF;i++) if($i==\"dev\") print $(i+1)}' | head -1")
        interface = stdout.read().decode().strip()
        
        if not interface:
            # 备用方案：获取第一个非lo接口
            stdin, stdout, stderr = ssh.exec_command("ip link show | grep -E '^[0-9]+: [^l][^o]' | head -1 | cut -d: -f2 | tr -d ' '")
            interface = stdout.read().decode().strip()
        
        logging.info(f"节点 {node_idx} 的网络接口: {interface}")
        return interface
        
    except Exception as e:
        logging.error(f"获取节点 {node_idx} 网络接口失败: {e}")
        return "eth0"  # 默认接口名
    finally:
        if 'ssh' in locals():
            ssh.close()


def apply_transmission_delay(node_idx: int, delay_ms: int, variance_ms: int = 0) -> bool:
    """
    在指定节点上应用传输延迟
    
    参数:
        node_idx: 节点索引
        delay_ms: 延迟时间（毫秒）
        variance_ms: 延迟变化范围（毫秒）
    
    返回:
        bool: 操作是否成功
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[node_idx], username="ubuntu", password="Dwf12345")
        
        # 获取网络接口
        interface = get_network_interface(node_idx)
        
        # 构建tc命令
        if variance_ms > 0:
            tc_command = f"sudo tc qdisc add dev {interface} root netem delay {delay_ms}ms {variance_ms}ms"
        else:
            tc_command = f"sudo tc qdisc add dev {interface} root netem delay {delay_ms}ms"
        
        # 执行tc命令添加延迟
        stdin, stdout, stderr = ssh.exec_command(tc_command)
        exit_status = stdout.channel.recv_exit_status()
        
        if exit_status == 0:
            logging.info(f"节点 {node_idx} ({server_ip[node_idx]}) 已添加传输延迟: {delay_ms}ms ±{variance_ms}ms")
            return True
        else:
            error_output = stderr.read().decode()
            logging.error(f"节点 {node_idx} 添加传输延迟失败: {error_output}")
            return False
        
    except Exception as e:
        logging.error(f"节点 {node_idx} 添加传输延迟时出错: {e}")
        return False
    finally:
        if 'ssh' in locals():
            ssh.close()


def remove_transmission_delay(node_idx: int) -> bool:
    """
    移除指定节点的传输延迟
    
    参数:
        node_idx: 节点索引
    
    返回:
        bool: 操作是否成功
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip[node_idx], username="ubuntu", password="Dwf12345")
        
        # 获取网络接口
        interface = get_network_interface(node_idx)
        
        # 删除tc规则
        tc_command = f"sudo tc qdisc del dev {interface} root"
        stdin, stdout, stderr = ssh.exec_command(tc_command)
        exit_status = stdout.channel.recv_exit_status()
        
        if exit_status == 0:
            logging.info(f"节点 {node_idx} ({server_ip[node_idx]}) 的传输延迟已移除")
            return True
        else:
            # 可能没有规则可删除，这通常不是错误
            logging.info(f"节点 {node_idx} ({server_ip[node_idx]}) 没有需要删除的传输延迟规则")
            return True
        
    except Exception as e:
        logging.error(f"节点 {node_idx} 移除传输延迟时出错: {e}")
        return False
    finally:
        if 'ssh' in locals():
            ssh.close()


def get_random_half_nodes() -> List[int]:
    """
    获取随机一半节点（向下取整）且非0号节点
    
    返回:
        List[int]: 被选中的节点索引列表
    """
    # 排除0号节点，从1号节点开始
    available_nodes = list(range(1, node_num))
    
    # 计算一半节点数量（向下取整）
    half_count = len(available_nodes) // 2
    
    # 随机选择一半节点
    selected_nodes = random.sample(available_nodes, half_count)
    
    logging.info(f"可用节点: {available_nodes}")
    logging.info(f"随机选择 {half_count} 个节点进行传输延迟设置: {selected_nodes}")
    
    return selected_nodes


def apply_transmission_delay_to_selected_nodes(selected_nodes: List[int], delay_ms: int, variance_ms: int = 0):
    """
    为选中的节点应用传输延迟
    
    参数:
        selected_nodes: 被选中的节点索引列表
        delay_ms: 延迟时间（毫秒）
        variance_ms: 延迟变化范围（毫秒）
    """
    logging.info(f"\n【开始应用传输延迟】延迟: {delay_ms}ms ±{variance_ms}ms")
    logging.info(f"目标节点: {selected_nodes}")
    
    # 并行为选中的节点添加延迟
    threads = []
    results = [False] * len(selected_nodes)
    
    def apply_delay_thread(idx, node_idx):
        results[idx] = apply_transmission_delay(node_idx, delay_ms, variance_ms)
    
    for idx, node_idx in enumerate(selected_nodes):
        t = threading.Thread(target=apply_delay_thread, args=(idx, node_idx))
        threads.append(t)
        t.start()
    
    # 等待所有操作完成
    for t in threads:
        t.join()
    
    success_count = sum(results)
    if success_count == len(selected_nodes):
        logging.info(f"【传输延迟应用完成】成功为 {len(selected_nodes)} 个节点添加传输延迟")
    else:
        logging.warning(f"【传输延迟应用部分完成】成功: {success_count}/{len(selected_nodes)} 个节点")


def remove_transmission_delay_from_selected_nodes(selected_nodes: List[int]):
    """
    移除选中节点的传输延迟
    
    参数:
        selected_nodes: 被选中的节点索引列表
    """
    logging.info(f"\n【开始移除传输延迟】目标节点: {selected_nodes}")
    
    # 并行移除选中节点的延迟
    threads = []
    results = [False] * len(selected_nodes)
    
    def remove_delay_thread(idx, node_idx):
        results[idx] = remove_transmission_delay(node_idx)
    
    for idx, node_idx in enumerate(selected_nodes):
        t = threading.Thread(target=remove_delay_thread, args=(idx, node_idx))
        threads.append(t)
        t.start()
    
    # 等待所有操作完成
    for t in threads:
        t.join()
    
    success_count = sum(results)
    if success_count == len(selected_nodes):
        logging.info(f"【传输延迟移除完成】成功移除 {len(selected_nodes)} 个节点的传输延迟")
    else:
        logging.warning(f"【传输延迟移除部分完成】成功: {success_count}/{len(selected_nodes)} 个节点")


def performance_imbalance_scenario(bat_path: str = "test.bat", 
                                  test_result_file_path: str = "test_result.txt",
                                  storing_path: str = "single_run_results") -> None:
    """
    运行单次性能不平衡场景
    
    参数:
        bat_path: 测试脚本路径
        test_result_file_path: 单次测试结果文件路径
        storing_path: 结果输出路径
    """
    current_time = int(time.time())
    output_store_path = f"{storing_path}\\result_performance_imbalance_{current_time}\\single_run.json"
    
    logging.info(f"\n{'='*80}")
    logging.info(f"开始单次性能不平衡场景实验")
    logging.info(f"传输延迟配置: {TRANSMISSION_DELAY_MS}ms ±{DELAY_VARIANCE_MS}ms")
    logging.info(f"{'='*80}")
    
    # 修改DB_SWITCH配置
    logging.info("\n【配置数据库】修改benchmark配置中的DB_SWITCH...")
    if not modify_db_switch():
        logging.error("❌ 修改DB_SWITCH失败，实验终止")
        return None
    
    # 调用性能不平衡场景函数
    exp_result = performance_imbalance_scenario_single_run(
        bat_path=bat_path,
        test_result_file_path=test_result_file_path,
        output_store_path=output_store_path
    )
    
    logging.info(f"\n实验完成！结果已保存到 {output_store_path}")
    return exp_result


def performance_imbalance_scenario_single_run(bat_path, test_result_file_path, output_store_path):
    """
    单次性能不平衡场景主函数：清理→启动→等待20分钟→异常测试(期间对随机一半节点进行传输延迟)→结果存储→停止系统
    
    参数：
        bat_path: str - 测试用bat文件的完整路径
        test_result_file_path: str - 单次测试结果文件的完整路径
        output_store_path: str - 最终测试结果集合的存储路径
    
    返回：
        dict - 异常测试的结果集合（含状态信息）
    """
    # 随机选择一半节点（向下取整）且非0号节点
    selected_nodes = get_random_half_nodes()
    
    # 初始化测试结果集合
    all_test_results = {
        "scenario_name": "performance_imbalance_scenario_single_run",
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "node_count": node_num,
        "server_ips": server_ip,
        "transmission_delay_ms": TRANSMISSION_DELAY_MS,
        "delay_variance_ms": DELAY_VARIANCE_MS,
        "selected_nodes": selected_nodes,  # 记录被选中的节点
        "test_results": [],
        "end_time": "",
        "status": "running"
    }

    try:
        # -------------------------- 1. 清理所有节点 --------------------------
        logging.info("【步骤1/5】清理所有节点...")
        clean_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            clean_threads.append(t)

        time.sleep(10)
        logging.info("【步骤1/5】所有节点清理完成")

        # 同时移除所有节点的传输延迟（预防性清理）
        logging.info("【步骤1/5】预防性移除传输延迟...")
        for node_idx in selected_nodes:
            remove_transmission_delay(node_idx)

        # -------------------------- 2. 启动所有ConfigNode --------------------------
        logging.info("\n【步骤2/5】启动所有ConfigNode...")
        config_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startConfigNode, args=(idx,))
            t.start()
            config_threads.append(t)
        time.sleep(60)
        logging.info("【步骤2/5】所有ConfigNode启动完成")

        # -------------------------- 3. 启动所有DataNode --------------------------
        logging.info("\n【步骤3/5】启动所有DataNode...")
        data_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startDataNode, args=(idx,))
            t.start()
            data_threads.append(t)
        time.sleep(60)
        logging.info("【步骤3/5】所有DataNode启动完成")

        # -------------------------- 4. 启动节点监控系统 --------------------------
        logging.info("\n【步骤4/5】启动节点监控系统（Prometheus + Grafana）...")
        start_monitoring_system()
        logging.info("【步骤4/5】节点监控系统启动完成")

        # -------------------------- 5. 异常测试：等待20分钟后开始，期间对随机一半节点进行传输延迟 --------------------------
        logging.info("\n【步骤5/5】等待20分钟后开始异常测试（期间对随机一半节点进行传输延迟操作）...")
        time.sleep(20 * 60)  # 等待20分钟
        
        # 创建异步执行传输延迟操作的线程
        def transmission_delay_operation():
            logging.info(f"等待10分钟后对选中节点应用传输延迟（{TRANSMISSION_DELAY_MS}ms ±{DELAY_VARIANCE_MS}ms）...")
            time.sleep(10 * 60)  # 等待10分钟
            
            logging.info("开始对选中节点应用传输延迟...")
            apply_transmission_delay_to_selected_nodes(selected_nodes, TRANSMISSION_DELAY_MS, DELAY_VARIANCE_MS)
            
            logging.info("等待15分钟后移除选中节点的传输延迟...")
            time.sleep(15 * 60)  # 等待15分钟
            
            logging.info("开始移除选中节点的传输延迟...")
            remove_transmission_delay_from_selected_nodes(selected_nodes)
            logging.info("传输延迟操作完成")
        
        # 启动传输延迟操作线程
        operation_thread = threading.Thread(target=transmission_delay_operation)
        operation_thread.start()
        
        # 同时开始异常测试
        logging.info("开始异常测试...")
        abnormal_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        abnormal_test["test_phase"] = "imbalance"
        abnormal_test["phase_description"] = f"性能不平衡测试（异常状态 - 延迟: {TRANSMISSION_DELAY_MS}ms ±{DELAY_VARIANCE_MS}ms，影响节点: {selected_nodes}）"
        abnormal_test["transmission_delay_ms"] = TRANSMISSION_DELAY_MS
        abnormal_test["delay_variance_ms"] = DELAY_VARIANCE_MS
        abnormal_test["affected_nodes"] = selected_nodes
        all_test_results["test_results"].append(abnormal_test)
        
        # 等待传输延迟操作完成
        operation_thread.join()
        logging.info("【步骤5/5】异常测试和传输延迟操作均完成")

        # -------------------------- 6. 更新场景状态，存储结果 --------------------------
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
        
        # 异常情况下也要移除传输延迟
        try:
            remove_transmission_delay_from_selected_nodes(selected_nodes)
        except Exception as restore_error:
            logging.warning(f"⚠️ 移除传输延迟时出错: {restore_error}")
        
        # 存储异常状态下的结果
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        logging.warning(f"⚠️  已将异常状态下的结果存储到 {output_store_path}")

    finally:
        # 最终清理：确保传输延迟移除并停止所有节点
        try:
            logging.info("\n【最终步骤】确保传输延迟移除...")
            remove_transmission_delay_from_selected_nodes(selected_nodes)
        except Exception as e:
            logging.warning(f"⚠️ 最终移除传输延迟时出错: {e}")
        
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
    performance_imbalance_scenario()
