import threading
import time
import logging
import os
from config import node_num, abnormal_scenario, INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH, DB_TYPE
from tools import startConfigNode, startDataNode, modify_db_switch
from node_outage import node_outage_scenario
from symmetric_network_partition import symmetric_network_partition_scenario
from asymmetric_network_partition import asymmetric_network_partition_scenario
from abnormal_transmission import abnormal_transmission_scenario
from over_load import over_load_scenario
from out_of_order import out_of_order_scenario
from performance_imbalance import performance_imbalance_scenario

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

if __name__ == "__main__":
    stop_event = threading.Event()  # 保留原stop_event，供监控函数（如monitor_and_restart）使用
    logging.info(f"{'='*60}")
    logging.info(f"启动程序 | 数据库类型：{DB_TYPE} | 异常场景：{abnormal_scenario} | 节点数量：{node_num}")
    logging.info(f"{'='*60}")
    
    # 在运行测试前，根据DB_TYPE修改benchmark配置文件中的DB_SWITCH参数
    logging.info(f"\n【配置数据库】根据DB_TYPE={DB_TYPE}修改benchmark配置...")
    if not modify_db_switch():
        logging.error("❌ 修改DB_SWITCH失败，程序终止")
        exit(1)

    if abnormal_scenario == "node_outage":
        # 执行单次节点宕机场景，自动触发两次测试并输出结果
        logging.info("开始执行单次节点宕机测试流程...")
        node_outage_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "symmetric_network_partition":
        logging.info("开始执行对称网络分区测试流程...")
        symmetric_network_partition_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "asymmetric_network_partition":
        logging.info("开始执行非对称网络分区测试流程...")
        asymmetric_network_partition_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "abnormal_transmission":
        logging.info("开始执行传输时间异常测试流程...")
        abnormal_transmission_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "over_load":
        logging.info("开始执行过载测试流程...")
        over_load_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "out_of_order":
        logging.info("开始执行消息乱序测试流程...")
        out_of_order_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "performance_imbalance":
        logging.info("开始执行性能不平衡测试流程...")
        performance_imbalance_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    else:
        # 默认场景：仅启动所有节点，不执行测试
        logging.info("\nℹ️  无异常场景（或场景配置错误），仅启动所有节点...")
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
        logging.info("\n✅ 所有节点（ConfigNode + DataNode）启动完成，程序保持运行...")
        
        config_threads = []
        data_threads = []
        restart_count = [0] * node_num
