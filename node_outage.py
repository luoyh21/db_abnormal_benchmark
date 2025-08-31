import json
import paramiko
import time
import threading
import random
from config import node_num,server_ip
from main import startConfigNode, startDataNode,stopNode,monitor_and_restart,run_bat_and_parse


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
        for t in clean_threads:
            t.join()
        print("【步骤1/7】所有节点清理完成")

        # -------------------------- 2. 启动所有ConfigNode --------------------------
        print("\n【步骤2/7】启动所有ConfigNode...")
        config_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startConfigNode, args=(idx,))
            t.start()
            config_threads.append(t)
        for t in config_threads:
            t.join()
        print("【步骤2/7】所有ConfigNode启动完成")

        # -------------------------- 3. 启动所有DataNode --------------------------
        print("\n【步骤3/7】启动所有DataNode...")
        data_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startDataNode, args=(idx,))
            t.start()
            data_threads.append(t)
        for t in data_threads:
            t.join()
        print("【步骤3/7】所有DataNode启动完成")

        # -------------------------- 4. 启动节点监控 --------------------------
        print("\n【步骤4/7】启动节点监控线程...")
        restart_count = [0] * node_num
        monitor_thread = threading.Thread(
            target=monitor_and_restart,
            args=(config_threads, data_threads, restart_count),
            daemon=True  # 设为守护线程，主流程结束时自动退出
        )
        monitor_thread.start()
        print("【步骤4/7】监控线程启动完成")

        # -------------------------- 5. 第一次测试：节点启动5分钟后 --------------------------
        print("\n【步骤5/7】等待5分钟，准备第一次测试（节点启动后稳定测试）...")
        time.sleep(300)  # 5分钟 = 300秒
        first_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path,
            test_label="第一次测试：节点启动5分钟后（正常状态）"
        )
        all_test_results["test_results"].append(first_test)

        # -------------------------- 6. 第二次测试：随机DataNode停止1分钟后 --------------------------
        # 随机选择一个DataNode宕机
        fail_idx = random.randint(0, node_num - 1)
        print(f"\n【步骤6/7】随机宕机 DataNode {fail_idx}...")
        stopNode(fail_idx, only_datanode=True)
        print(f"【步骤6/7】等待1分钟，准备第二次测试（DataNode {fail_idx} 停止后）...")
        time.sleep(60)  # 1分钟 = 60秒
        second_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path,
            test_label=f"第二次测试：DataNode {fail_idx} 停止1分钟后（异常状态）"
        )
        all_test_results["test_results"].append(second_test)

        # -------------------------- 7. 第三次测试：DataNode重启1分钟后 --------------------------
        print(f"\n【步骤7/7】重启 DataNode {fail_idx}...")
        startDataNode(fail_idx)
        print(f"【步骤7/7】等待1分钟，准备第三次测试（DataNode {fail_idx} 重启后）...")
        time.sleep(60)  # 1分钟 = 60秒
        third_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path,
            test_label=f"第三次测试：DataNode {fail_idx} 重启1分钟后（恢复状态）"
        )
        all_test_results["test_results"].append(third_test)

        # -------------------------- 8. 更新场景状态，存储结果 --------------------------
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "finished"  # 场景正常完成
        print(f"\n{'='*60}")
        print(f"场景执行完成！开始将结果写入存储文件：{output_store_path}")
        
        # 写入JSON文件（缩进2格，便于人类阅读）
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
        for t in stop_threads:
            t.join()
        print("【最终步骤】所有节点停止完成")

    # 返回完整的测试结果集合（便于后续程序调用）
    return all_test_results