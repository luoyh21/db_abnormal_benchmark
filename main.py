import threading
import time
from config import node_num, abnormal_scenario, server_ip,INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH
from tools import startConfigNode, startDataNode
from node_outage import node_outage_scenario
from symmetric_network_partition import symmetric_network_partition

if __name__ == "__main__":
    stop_event = threading.Event()  # 保留原stop_event，供监控函数（如monitor_and_restart）使用
    print(f"{'='*60}")
    print(f"启动程序 | 异常场景：{abnormal_scenario} | 节点数量：{node_num}")
    print(f"{'='*60}")

    if abnormal_scenario == "node_outage":
        # 执行单次节点宕机场景，自动触发两次测试并输出结果
        print("开始执行单次节点宕机测试流程...")
        node_outage_scenario(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
    elif abnormal_scenario == "symmetric_network_partition":
        print("开始执行对称网络分区测试流程...")
        symmetric_network_partition(INPUT_BAT_PATH, INPUT_TEST_RESULT_PATH, OUTPUT_STORE_PATH)
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
            