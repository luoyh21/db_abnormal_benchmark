import json
import time
import threading
import random
import logging
import os
import shutil
from config import node_num, server_ip, abnormal_scenario, OUTPUT_STORE_PATH, BENCHMARK_CONFIG_PATH
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

def modify_benchmark_config_for_write_only():
    """
    修改benchmark配置文件，设置为仅写入模式
    修改OPERATION_PROPORTION为1:0:0:0:0:0:0:0:0:0:0:0
    同时修改LOOP为1500
    """
    try:
        # 备份原始配置文件
        backup_path = BENCHMARK_CONFIG_PATH + ".backup"
        if not os.path.exists(backup_path):
            shutil.copy2(BENCHMARK_CONFIG_PATH, backup_path)
            logging.info(f"已备份原始配置文件到: {backup_path}")
        
        # 读取配置文件
        with open(BENCHMARK_CONFIG_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 修改配置参数
        modified_lines = []
        for line in lines:
            if line.strip().startswith('OPERATION_PROPORTION'):
                modified_lines.append("OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0:0\n")
                logging.info("修改OPERATION_PROPORTION: 设置为仅写入模式")
            elif line.strip().startswith('LOOP'):
                modified_lines.append("LOOP=1500\n")
                logging.info("修改LOOP: 设置为1500")
            else:
                modified_lines.append(line)
        
        # 写入修改后的配置
        with open(BENCHMARK_CONFIG_PATH, 'w', encoding='utf-8') as f:
            f.writelines(modified_lines)
        
        logging.info("✅ 配置文件修改完成，已设置为仅写入模式，LOOP=1500")
        return True
        
    except Exception as e:
        logging.error(f"❌ 修改配置文件时出错: {e}")
        return False

def modify_benchmark_config_for_disorder():
    """
    修改benchmark配置文件，开启消息乱序
    修改IS_OUT_OF_ORDER为TRUE
    """
    try:
        # 读取配置文件
        with open(BENCHMARK_CONFIG_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 修改配置参数
        modified_lines = []
        for line in lines:
            if line.strip().startswith('IS_OUT_OF_ORDER'):
                modified_lines.append("IS_OUT_OF_ORDER=true\n")
                logging.info("修改IS_OUT_OF_ORDER: 设置为TRUE，开启消息乱序")
            else:
                modified_lines.append(line)
        
        # 写入修改后的配置
        with open(BENCHMARK_CONFIG_PATH, 'w', encoding='utf-8') as f:
            f.writelines(modified_lines)
        
        logging.info("✅ 配置文件修改完成，已开启消息乱序")
        return True
        
    except Exception as e:
        logging.error(f"❌ 修改配置文件时出错: {e}")
        return False

def restore_benchmark_config():
    """
    恢复benchmark配置文件到原始状态
    包括恢复LOOP=15000和OPERATION_PROPORTION的原始值
    """
    try:
        backup_path = BENCHMARK_CONFIG_PATH + ".backup"
        if os.path.exists(backup_path):
            shutil.copy2(backup_path, BENCHMARK_CONFIG_PATH)
            logging.info("✅ 配置文件已恢复到原始状态（包括LOOP=15000）")
            return True
        else:
            logging.warning("⚠️  未找到备份文件，无法恢复配置")
            return False
    except Exception as e:
        logging.error(f"❌ 恢复配置文件时出错: {e}")
        return False

def out_of_order_scenario(bat_path: str = "test.bat", 
                             test_result_file_path: str = "test_result.txt",
                             storing_path: str = "single_run_results") -> None:
    """
    运行单次消息乱序场景
    
    参数:
        bat_path: 测试脚本路径
        test_result_file_path: 单次测试结果文件路径
        storing_path: 结果输出路径
    """
    current_time = int(time.time())
    output_store_path = f"{storing_path}\\result_out_of_order_{current_time}\\single_run.json"
    
    logging.info(f"\n{'='*80}")
    logging.info(f"开始单次消息乱序场景实验")
    logging.info(f"{'='*80}")
    
    # 修改DB_SWITCH配置
    logging.info("\n【配置数据库】修改benchmark配置中的DB_SWITCH...")
    if not modify_db_switch():
        logging.error("❌ 修改DB_SWITCH失败，实验终止")
        return None
    
    # 调用消息乱序场景函数
    exp_result = out_of_order_scenario_single_run(
        bat_path=bat_path,
        test_result_file_path=test_result_file_path,
        output_store_path=output_store_path
    )
    
    logging.info(f"\n实验完成！结果已保存到 {output_store_path}")
    return exp_result

def out_of_order_scenario_single_run(bat_path, test_result_file_path, output_store_path):
    """
    单次消息乱序场景主函数：清理→启动→等待20分钟→第一次测试(仅写入)→修改配置→第二次测试(乱序模式)→恢复配置→停止系统
    参数：
        bat_path: str - 测试用bat文件的完整路径
        test_result_file_path: str - 单次测试结果文件的完整路径
        output_store_path: str - 最终测试结果集合的存储路径
    返回：
        dict - 两次测试的结果集合（含状态信息）
    """
    # 初始化两次测试的结果集合
    all_test_results = {
        "scenario_name": "out_of_order_scenario_single_run",  # 场景名称
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),  # 场景开始时间
        "node_count": node_num,  # 节点数量
        "server_ips": server_ip,  # 服务器IP列表
        "test_results": [],  # 存储两次测试的具体结果
        "end_time": "",  # 场景结束时间（最后赋值）
        "status": "running"  # 场景整体状态：running/finished/failed
    }

    try:
        # -------------------------- 1. 清理所有节点 --------------------------
        logging.info("【步骤1/7】清理所有节点...")
        clean_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=stopNode, args=(idx,))
            t.start()
            clean_threads.append(t)

        time.sleep(10)
        logging.info("【步骤1/7】所有节点清理完成")

        # -------------------------- 2. 启动所有ConfigNode --------------------------
        logging.info("\n【步骤2/7】启动所有ConfigNode...")
        config_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startConfigNode, args=(idx,))
            t.start()
            config_threads.append(t)
        time.sleep(60)
        logging.info("【步骤2/7】所有ConfigNode启动完成")

        # -------------------------- 3. 启动所有DataNode --------------------------
        logging.info("\n【步骤3/7】启动所有DataNode...")
        data_threads = []
        for idx in range(node_num):
            t = threading.Thread(target=startDataNode, args=(idx,))
            t.start()
            data_threads.append(t)
        time.sleep(60)
        logging.info("【步骤3/7】所有DataNode启动完成")

        # -------------------------- 4. 启动节点监控系统 --------------------------
        logging.info("\n【步骤4/7】启动节点监控系统（Prometheus + Grafana）...")
        start_monitoring_system()
        logging.info("【步骤4/7】节点监控系统启动完成")

        # -------------------------- 5. 第一次测试：仅写入模式测试 --------------------------
        logging.info("\n【步骤5/7】等待20分钟，准备第一次测试（仅写入模式测试）...")
        time.sleep(20 * 60)  # 等待20分钟
        
        # 修改配置为仅写入模式
        logging.info("修改配置为仅写入模式...")
        if not modify_benchmark_config_for_write_only():
            logging.error("修改配置文件失败")
            raise Exception("修改配置文件失败")
        
        first_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        # 为第一次测试添加状态标识（仅写入状态）
        first_test["test_phase"] = "write_only"
        first_test["phase_description"] = "仅写入模式测试（基准写入性能）"
        all_test_results["test_results"].append(first_test)
        logging.info("【步骤5/7】第一次测试完成")

        # -------------------------- 6. 修改配置并执行乱序测试 --------------------------
        logging.info("\n【步骤6/7】等待30分钟后开始第二次测试（期间进行配置修改操作）...")
        time.sleep(30 * 60)  # 等待30分钟
        
        # 创建异步执行配置修改操作的线程
        def config_modification_operation():
            logging.info("等待10分钟后修改配置文件为乱序模式...")
            time.sleep(10 * 60)  # 等待10分钟
            
            logging.info("开始修改配置文件为乱序模式...")
            if not modify_benchmark_config_for_disorder():
                logging.error("修改配置文件失败")
                return
            
            logging.info("等待15分钟后恢复配置文件...")
            time.sleep(15 * 60)  # 等待15分钟
            
            logging.info("开始恢复配置文件...")
            if not restore_benchmark_config():
                logging.warning("配置文件恢复失败")
            logging.info("配置修改操作完成")
        
        # 启动配置修改操作线程
        operation_thread = threading.Thread(target=config_modification_operation)
        operation_thread.start()
        
        # 同时开始第二次测试
        logging.info("开始第二次测试...")
        second_test = run_bat_and_parse(
            bat_path=bat_path,
            result_file_path=test_result_file_path
        )
        # 为第二次测试添加状态标识（乱序状态）
        second_test["test_phase"] = "disorder"
        second_test["phase_description"] = "消息乱序模式测试（乱序写入性能）"
        all_test_results["test_results"].append(second_test)
        
        # 等待配置修改操作完成
        operation_thread.join()
        logging.info("【步骤6/7】第二次测试和配置修改操作均完成")

        # -------------------------- 7. 更新状态并存储结果 --------------------------
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "finished"  # 场景正常完成
        logging.info(f"\n{'='*60}")
        logging.info(f"场景执行完成！开始将结果写入存储文件：{output_store_path}")
        
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ 结果已成功存储到 {output_store_path}")

    except Exception as e:
        # 捕获场景执行中的全局异常，更新状态并存储
        error_msg = f"场景执行异常：{str(e)}"
        logging.error(f"\n❌ {error_msg}")
        all_test_results["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        all_test_results["status"] = "failed"
        all_test_results["error_msg"] = error_msg
        
        # 尝试恢复配置文件
        try:
            restore_benchmark_config()
        except:
            logging.warning("⚠️  异常情况下配置文件恢复失败")
        
        # 即使异常，也尝试存储已有的测试结果
        os.makedirs(os.path.dirname(output_store_path), exist_ok=True)
        with open(output_store_path, 'w', encoding='utf-8') as f:
            json.dump(all_test_results, f, ensure_ascii=False, indent=2)
        logging.warning(f"⚠️  已将异常状态下的结果存储到 {output_store_path}")

    # 返回完整的测试结果集合（便于后续程序调用）
    return all_test_results

if __name__ == "__main__":
    # 测试消息乱序场景
    from config import INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH
    out_of_order_scenario(
        bat_path=INPUT_BAT_PATH,
        test_result_file_path=INPUT_TEST_RESULT_PATH,
        storing_path=OUTPUT_STORE_PATH
    )
