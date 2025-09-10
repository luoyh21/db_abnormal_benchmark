import json
import paramiko
import time
import threading
import random
from config import node_num,server_ip,abnormal_scenario
from typing import List, Dict, Any
import os
from tools import format_matrix_for_output, calculate_phase_averages,startConfigNode, startDataNode,stopNode,monitor_and_restart,run_bat_and_parse


def run_repeated_node_outage_scenarios(repeat_count: int = 3, 
                                      bat_path: str = "test.bat", 
                                      test_result_file_path: str = "test_result.txt",
                                      storing_path: str = "final_average_results") -> None:
    """
    重复运行节点宕机场景并计算平均值
    
    参数:
        repeat_count: 重复实验次数
        bat_path: 测试脚本路径
        test_result_file_path: 单次测试结果文件路径
        storing_path: 结果输出路径
    """
    current_time = int(time.time())
    final_output_path = f"{storing_path}\\result_{abnormal_scenario}_{current_time}\\final_result.txt"
    output_store_path = f"{storing_path}\\result_{abnormal_scenario}_{current_time}\\exp{exp_idx+1}.json"
    os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
    # 存储所有实验的结果，按测试阶段分组
    all_phase_results = {
        "phase1": [],  # 第一次测试：节点启动后稳定测试
        "phase2": [],  # 第二次测试：DataNode停止后
        "phase3": []   # 第三次测试：DataNode重启后
    }
    
    # 运行多次实验
    for exp_idx in range(repeat_count):
        print(f"\n{'='*80}")
        print(f"开始第 {exp_idx+1}/{repeat_count} 次节点宕机场景实验")
        print(f"{'='*80}")
        
        # 调用节点宕机场景函数
        exp_result = node_outage_scenario(
            bat_path=bat_path,
            test_result_file_path=test_result_file_path,
            #输出以当前时间创建文件夹和文件
            output_store_path=output_store_path
        )
        
        # 检查实验结果是否完整
        if exp_result["status"] == "finished" and len(exp_result["test_results"]) == 3:
            all_phase_results["phase1"].append(exp_result["test_results"][0])
            all_phase_results["phase2"].append(exp_result["test_results"][1])
            all_phase_results["phase3"].append(exp_result["test_results"][2])
            print(f"第 {exp_idx+1} 次实验完成并记录结果")
        else:
            print(f"第 {exp_idx+1} 次实验结果不完整或失败，已跳过")
    
    # 计算每个测试阶段的平均值
    phase_averages = {}
    for phase, results in all_phase_results.items():
        if results:
            phase_averages[phase] = calculate_phase_averages(results)
            print(f"{phase} 阶段共收集 {len(results)} 组有效数据，已计算平均值")
        else:
            print(f"{phase} 阶段没有有效数据，无法计算平均值")
    
    # 格式化并保存最终平均值结果
    with open(final_output_path, 'w', encoding='utf-8') as f:
        # 写入每个阶段的平均矩阵
        for phase_idx, (phase, avg_data) in enumerate(phase_averages.items(), 1):
            phase_name = {
                "phase1": "节点启动后稳定测试",
                "phase2": "DataNode停止后测试",
                "phase3": "DataNode重启后测试"
            }[phase]
            
            f.write(f"{'='*100}\n")
            f.write(f"===== 第 {phase_idx} 阶段测试平均值 ({phase_name}) - 基于 {len(all_phase_results[phase])} 次重复实验 =====\n")
            f.write(f"{'='*100}\n\n")
            
            # 写入Result Matrix
            result_lines = format_matrix_for_output(avg_data, "result_matrix")
            f.write('\n'.join(result_lines) + '\n\n\n')
            
            # 写入Latency Matrix
            latency_lines = format_matrix_for_output(avg_data, "latency_matrix")
            f.write('\n'.join(latency_lines) + '\n\n')
    
    print(f"\n所有实验完成！平均值结果已按要求格式保存到 {final_output_path}")
    


def node_outage_scenario(bat_path, test_result_file_path, output_store_path):
    """
    节点宕机场景主函数：清理→启动→三次测试→结果存储→停止系统
    参数：
        bat_path: str - 测试用bat文件的完整路径（如"D:\\test\\run_test.bat"）
        test_result_file_path: str - 单次测试结果文件的完整路径（bat执行后生成的结果文件）
        output_store_path: str - 最终测试结果集合的存储路径（如"D:\\test\\all_results.json"）
    返回：
        dict - 三次测试的结果集合（含状态信息）
    """
    # 初始化三次测试的结果集合
    all_test_results = {
        "scenario_name": "node_outage_scenario",  # 场景名称
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),  # 场景开始时间
        "node_count": node_num,  # 节点数量
        "server_ips": server_ip,  # 服务器IP列表
        "test_results": [],  # 存储三次测试的具体结果
        "end_time": "",  # 场景结束时间（最后赋值）
        "status": "running"  # 场景整体状态：running/finished/failed
    }

    try:
        # -------------------------- 1. 清理所有节点 --------------------------
        print("【步骤1/7】清理所有节点...")
        clean_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            clean_threads.append(t)

        time.sleep(10)

        print("【步骤1/7】所有节点清理完成")

        # -------------------------- 2. 启动所有ConfigNode --------------------------
        print("\n【步骤2/7】启动所有ConfigNode...")
        config_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startConfigNode, args=(idx,))
            t.start()
            config_threads.append(t)
        time.sleep(60)

        print("【步骤2/7】所有ConfigNode启动完成")

        # -------------------------- 3. 启动所有DataNode --------------------------
        print("\n【步骤3/7】启动所有DataNode...")
        data_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startDataNode, args=(idx,))
            t.start()
            data_threads.append(t)
        time.sleep(60)

        print("【步骤3/7】所有DataNode启动完成")

        # -------------------------- 4. 启动节点监控 --------------------------
        print("\n【步骤4/7】启动节点监控线程...")
        # restart_count = [0] * node_num
        # monitor_thread = threading.Thread(
        #     target=monitor_and_restart,
        #     args=(config_threads, data_threads, restart_count),
        #     daemon=True  # 设为守护线程，主流程结束时自动退出
        # )
        # monitor_thread.start()
        print("【步骤4/7】监控线程启动完成")

        # -------------------------- 5. 第一次测试：节点启动5分钟后 --------------------------
        print("\n【步骤5/7】等待30分钟，准备第一次测试（节点启动后稳定测试）...")
        time.sleep(60)
        first_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        all_test_results["test_results"].append(first_test)

        # -------------------------- 6. 第二次测试：随机DataNode停止30分钟后 --------------------------
        # 随机选择一个DataNode宕机(不停止作为测试启动的DataNode 0)
        fail_idx = random.randint(1, node_num - 1)
        print(f"\n【步骤6/7】随机宕机 DataNode {fail_idx}...")
        fail_stop_thread = threading.Thread(target=stopNode, args=(fail_idx, True)) # 只停止DataNode
        fail_stop_thread.start()
        time.sleep(10)

        print(f"【步骤6/7】等待30分钟，准备第二次测试（DataNode {fail_idx} 停止后）...")
        time.sleep(60)
        second_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        all_test_results["test_results"].append(second_test)

        # -------------------------- 7. 第三次测试：DataNode重启30分钟后 --------------------------
        print(f"\n【步骤7/7】重启 DataNode {fail_idx}...")
        fail_restart_thread = threading.Thread(target=startDataNode, args=(fail_idx,))
        fail_restart_thread.start()
        time.sleep(10)

        print(f"【步骤7/7】等待10分钟，准备第三次测试（DataNode {fail_idx} 重启后）...")
        time.sleep(60)
        third_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        all_test_results["test_results"].append(third_test)

        # -------------------------- 8. 更新场景状态，存储结果 --------------------------
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "finished"  # 场景正常完成
        print(f"\n{'='*60}")
        print(f"场景执行完成！开始将结果写入存储文件：{output_store_path}")
        
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        print(f"✅ 结果已成功存储到 {output_store_path}")

    except Exception as e:
        # 捕获场景执行中的全局异常，更新状态并存储
        error_msg = f"场景执行异常：{str(e)}"
        print(f"\n❌ {error_msg}")
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "failed"
        all_test_results["error_msg"] = error_msg
        # 即使异常，也尝试存储已有的测试结果
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        print(f"⚠️  已将异常状态下的结果存储到 {output_store_path}")

    finally:
        # 无论场景成功/失败，都停止所有节点
        print("\n【最终步骤】停止所有节点...")
        stop_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            stop_threads.append(t)
        time.sleep(10)
        print("【最终步骤】所有节点停止完成")
        # 关闭所有监控线程


    # 返回完整的测试结果集合（便于后续程序调用）
    return all_test_results