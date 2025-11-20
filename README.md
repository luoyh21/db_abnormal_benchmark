# 数据库异常场景基准测试工具

## 项目简介

`db_abnormal_benchmark` 是一个用于测试 IoTDB、TDengine 等时序数据库在异常场景下性能表现的基准测试工具。该工具能够模拟各种异常场景（如丢包、网络分区、节点宕机等），并给出在**正常-异常-正常**三阶段下的吞吐率、成功率、延迟等各项性能指标。

## 功能特性

- ✅ 支持多种异常场景测试
- ✅ 自动启动和停止数据库节点
- ✅ 自动启动 Prometheus 和 Grafana 监控系统
- ✅ 自动解析测试结果并生成结构化报告
- ✅ 支持 IoTDB 和 TDengine 两种数据库类型
- ✅ 三阶段测试流程：正常 → 异常 → 恢复

## 环境要求

### 测试控制机（运行本工具的主机）

- 需要安装 **IoT-benchmark** 工具
- Python 3.x
- 依赖包：`paramiko`（用于 SSH 远程操作）

### 测试目标机（运行数据库的服务器）

- 安装目标数据库（IoTDB 或 TDengine）
- 安装 **Prometheus**（用于指标收集）
- 安装 **Grafana**（用于可视化监控）
- 配置 SSH 访问权限

### IoT-benchmark 配置要求

- 将 IoT-benchmark 的总操作次数设为 **18000 次左右**，使系统测试运行时间超过 **30 分钟**

## 快速开始

### 1. 配置文件设置

复制配置文件模板并修改：

```bash
cp config.example config.py
```

编辑 `config.py`，根据实际情况配置参数


### 2. 运行测试

```bash
python main.py
```

## 支持的测试场景

根据 `main.py` 中的配置，本工具支持以下 7 种异常场景测试：

### 1. 节点宕机（node_outage）

**场景描述**：模拟集群中某个节点突然宕机，测试系统在节点故障和恢复过程中的性能表现。

**测试流程**：
- 正常阶段：所有节点正常运行，进行基准测试
- 异常阶段：随机选择一个 DataNode 停止运行
- 恢复阶段：重启故障节点，系统恢复正常

### 2. 对称网络分区（symmetric_network_partition）

**场景描述**：模拟网络被对称分割成两个或多个独立分区，测试系统在网络分区情况下的可用性和一致性。

**测试流程**：
- 正常阶段：所有节点正常通信
- 异常阶段：通过防火墙规则将节点分成两个对称的分区
- 恢复阶段：移除防火墙规则，网络恢复正常

### 3. 非对称网络分区（asymmetric_network_partition）

**场景描述**：模拟非对称的网络分区，即某些节点与某些节点间的访问被阻断。

**测试流程**：
- 正常阶段：所有节点正常通信
- 异常阶段：创建非对称的网络分区（部分节点间访问阻断）
- 恢复阶段：恢复网络连接

### 4. 传输时间异常（abnormal_transmission）

**场景描述**：模拟网络传输延迟异常，测试系统在高延迟环境下的性能表现。

**测试流程**：
- 正常阶段：正常网络传输
- 异常阶段：为所有节点添加网络传输延迟（可配置延迟时间和变化范围）
- 恢复阶段：移除传输延迟

**配置参数**：
- `TRANSMISSION_DELAY_MS`：基础传输延迟时间（毫秒）
- `DELAY_VARIANCE_MS`：延迟变化范围（毫秒）

### 5. 过载（over_load）

**场景描述**：模拟系统过载场景，通过修改 benchmark 配置增加负载，测试系统在过载情况下的性能表现。

**测试流程**：
- 正常阶段：正常负载测试
- 异常阶段：增加系统负载（修改 benchmark 配置参数）
- 恢复阶段：恢复原始配置

### 6. 消息乱序（out_of_order）

**场景描述**：模拟消息乱序场景，测试系统处理乱序数据的能力。

**测试流程**：
- 正常阶段：正常顺序写入和查询
- 异常阶段：修改配置为乱序写入模式
- 恢复阶段：恢复正常顺序

### 7. 性能不平衡（performance_imbalance）

**场景描述**：模拟集群中部分节点性能下降（如网络延迟），测试系统在性能不平衡情况下的表现。

**测试流程**：
- 正常阶段：所有节点性能正常
- 异常阶段：随机选择一半节点添加传输延迟
- 恢复阶段：移除所有延迟

## 配置参数说明

### 基础配置

| 参数 | 说明 | 示例值 |
|------|------|--------|
| `node_num` | 集群节点数量 | `3` |
| `server_ip` | 服务器IP地址列表 | `['172.20.0.10', '172.20.0.15', '172.20.0.16']` |
| `abnormal_scenario` | 要测试的异常场景 | `"node_outage"` |
| `DB_TYPE` | 数据库类型 | `"IoTDB"` 或 `"TDengine"` |

### 异常场景配置

| 参数 | 说明 | 适用场景 | 默认值 |
|------|------|----------|--------|
| `TRANSMISSION_DELAY_MS` | 传输延迟时间（毫秒） | `abnormal_transmission`, `performance_imbalance` | `100` |
| `DELAY_VARIANCE_MS` | 延迟变化范围（毫秒） | `abnormal_transmission`, `performance_imbalance` | `10` |

### 路径配置


- `BENCHMARK_BASE_PATH`：IoT-benchmark 基础路径
- `INPUT_BAT_PATH`：benchmark 执行脚本路径
- `INPUT_TEST_RESULT_PATH`：测试结果日志路径
- `BENCHMARK_CONFIG_PATH`：benchmark 配置文件路径
- `OUTPUT_STORE_PATH`：测试结果存储路径

## 测试结果

### 结果存储位置

测试结果存储在 `OUTPUT_STORE_PATH` 目录下，每个测试会生成一个带时间戳的目录：

```
result/
  └── result_{场景名称}_{时间戳}/
      ├── single_run.json      # 测试结果JSON文件
      └── info.log            # 测试过程日志
```

### 结果文件格式

每个 `single_run.json` 文件包含以下信息：

```json
{
  "scenario_name": "场景名称",
  "start_time": "开始时间",
  "node_count": 节点数量,
  "server_ips": ["IP地址列表"],
  "test_results": [
    {
      "test_phase": "normal|abnormal|recovery",
      "phase_description": "阶段描述",
      "result_matrix": [
        "结果矩阵（包含吞吐率、成功率等）"
      ],
      "latency_matrix": [
        "延迟矩阵（包含各种延迟指标）"
      ]
    }
  ]
}
```

### 结果矩阵说明

#### Result Matrix（结果矩阵）

包含以下指标：
- `okOperation`：成功操作数
- `okPoint`：成功数据点数
- `failOperation`：失败操作数
- `failPoint`：失败数据点数
- `throughput(point/s)`：吞吐率（点/秒）

#### Latency Matrix（延迟矩阵）

包含以下延迟指标：
- `AVG`：平均延迟
- `MIN`：最小延迟
- `P10/P25/MEDIAN/P75/P90/P95/P99/P999`：百分位延迟
- `MAX`：最大延迟
- `SLOWEST_THREAD`：最慢线程延迟

### 三阶段测试结果

每个测试场景都会产生三个阶段的结果：

1. **正常阶段（normal）**：系统正常运行时的基准性能
2. **异常阶段（abnormal）**：异常场景下的性能表现
3. **恢复阶段（recovery）**：异常恢复后的性能表现

通过对比三个阶段的结果，可以评估系统在异常场景下的性能影响和恢复能力。

## Grafana 监控

### 自动启动监控系统

工具会在测试开始前自动启动 Prometheus 和 Grafana 监控系统：

- **Prometheus**：运行在 `http://{server_ip[0]}:9090`
- **Grafana**：运行在 `http://{server_ip[0]}:3000`

### 访问 Grafana

1. 打开浏览器访问：`http://{第一个节点IP}:3000`
2. 使用 Grafana 默认账号登录（通常为 `admin/admin`）
3. 在 Grafana 中可以：
   - 查看实时性能指标
   - 查询历史数据
   - 创建自定义仪表板
   - 分析系统在异常场景下的表现

### 监控指标

Grafana 通过 Prometheus 收集以下类型的指标：

- **吞吐率指标**：每秒操作数、每秒数据点数
- **延迟指标**：平均延迟、百分位延迟
- **错误率指标**：失败操作数、失败数据点数
- **系统资源指标**：CPU、内存、网络使用情况


## 使用示例

### 示例 1：测试节点宕机场景

```python
# config.py
node_num = 3
server_ip = ['172.20.0.10', '172.20.0.15', '172.20.0.16']
abnormal_scenario = "node_outage"
DB_TYPE = "IoTDB"
```

运行：
```bash
python main.py
```

### 示例 2：测试传输延迟异常

```python
# config.py
node_num = 3
server_ip = ['172.20.0.10', '172.20.0.15', '172.20.0.16']
abnormal_scenario = "abnormal_transmission"
DB_TYPE = "TDengine"
TRANSMISSION_DELAY_MS = 200  # 200ms延迟
DELAY_VARIANCE_MS = 50       # 50ms变化范围
```

## 注意事项

1. **SSH 配置**：确保测试控制机可以通过 SSH 访问所有测试目标机，默认用户名和密码在 `tools.py` 中配置

2. **路径配置**：确保 `BASE_PATH` 和所有自动生成的路径都正确指向实际文件位置

3. **测试时间**：每个测试场景包含三个阶段，总测试时间可能较长（通常超过 30 分钟），请确保有足够的测试时间

4. **资源占用**：测试过程中会启动多个数据库节点和监控服务，请确保服务器有足够的资源

5. **网络连接**：确保测试控制机和目标机之间的网络连接稳定

6. **数据库配置**：在运行测试前，确保目标数据库已正确安装和配置

## 故障排查

### 常见问题

1. **SSH 连接失败**
   - 检查网络连接
   - 验证 SSH 用户名和密码
   - 确认服务器IP地址正确

2. **节点启动失败**
   - 检查数据库安装路径
   - 查看日志文件 `result/info.log`
   - 确认数据库配置文件正确

3. **测试结果解析失败**
   - 检查 `INPUT_TEST_RESULT_PATH` 路径是否正确
   - 确认 benchmark 测试已正常完成
   - 查看日志了解详细错误信息

4. **Grafana 无法访问**
   - 确认 Grafana 服务已启动
   - 检查防火墙设置
   - 验证端口 3000 是否开放

## 日志文件

所有测试过程的日志都保存在 `OUTPUT_STORE_PATH/info.log` 文件中，包括：

- 节点启动/停止信息
- 测试执行进度
- 错误和警告信息
- 结果解析信息

## 贡献

欢迎提交 Issue 和 Pull Request 来改进本工具。

## 许可证

请参考项目根目录的 LICENSE 文件。
