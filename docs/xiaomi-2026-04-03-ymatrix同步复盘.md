# Xiaomi YMatrix 同步复盘

最后更新：2026-04-03

## 背景

- 排查对象：Iceberg -> YMatrix 同步任务
- 重点日志：
  - `xiaomi-logs/spark_log_040210.log`
  - `xiaomi-logs/spark_log_040218.log`
  - `xiaomi-logs/spark_log_040223.log`
  - `xiaomi-logs/spark_log_040300.log`
  - `xiaomi-logs/spark_log_040315.log`
- 当前目标：
  - 找清楚失败根因是数据、链路还是资源
  - 给出能落地的性能优化方案
  - 保留后续复盘所需的完整记录

## 故障结论

### 失败不是固定脏数据

- 多次失败分别报在不同字段上：
  - `hfaf2setposn`
  - `frntincartraw`
  - `fanpwmreq`
- 相同数据在后续重跑又成功，说明不符合“某条固定脏数据必然复现”的模式。

### 更像 gpfdist 链路中断或半行

- 失败日志中多次出现：
  - `missing data for column ...`
  - `GPFDIST transfer incomplete`
  - `Buffer transfer incomplete`
- 多次失败同时伴随 `Container preempted by scheduler`，说明 executor 在运行中被 YARN 抢占。
- 结合 connector 写路径是 `TEXT + \\t delimiter + \\n newline`，更符合“传输中断后 GP 端读到半行”的现象。

### 当前 segment 数

- 从日志确认 `nGpSegments=16`
- 本轮成功压测中 Spark 侧写入分区也是 `16`

## 040315 性能拆解

### 关键时间点

- `17:40:49` 开始读 Iceberg
- `17:40:57` 结束读 Iceberg
- `17:51:28` 开始写 YMatrix
- `18:05:14` 结束写 YMatrix

### 关键结论

- 纯读 Iceberg 只用了约 `8s`
- 真正大的耗时不在“读表”
- 最大头在 Spark 写前准备和 shuffle

### 关键 stage

- 写前空判断链路：
  - `ShuffleMapStage 0` 约 `63.96s`
  - `ShuffleMapStage 2` 约 `551.90s`
- 写入链路：
  - `ShuffleMapStage 6` 约 `62.20s`
  - `ShuffleMapStage 8` 约 `532.83s`
  - `ResultStage 11` 约 `220.28s`

### 当前判断

- 读 Iceberg 不是瓶颈
- 最终 `save()` 也不是唯一瓶颈
- 最大问题是：
  - 把空判断放在了高成本 shuffle 之后
  - 写入前 Spark 侧存在较重的数据重分布成本
  - 写入阶段仍有 executor 临时扩容和少量抢占

## 当前保留的交付物

### 日期后缀 jar

- 产物改为带日期后缀，便于排查现场时明确版本

## 本轮性能优化决策

本轮先只做低风险、收益高的两项：

1. 空判断前移到 `repartition` 之前
2. 保证写入开始前就有足够 executor，减少临时扩容

### 预估收益

- 保守预估总时长可从当前约 `24 分钟` 降到 `12 ~ 16 分钟`
- 主要收益来自第一项
- 第二项更多是稳态优化和削峰

## 已同步到仓库的推荐写法

### 写入前判空

- 先对过滤后的源 DataFrame 做：
  - `limit(1).take(1).nonEmpty`
- 只有非空才继续 `repartition`

### 写入前重分区

- 使用：
  - `repartition(writePartitions, col(distributedBy))`
- 不再把空判断动作放在 repartition 之后

## 后续验证计划

1. 在真实任务中只启用两项优化，记录总时长
2. 对比优化前后的：
   - 总时长
   - 写前 stage 耗时
   - `write_ymatrix` 耗时
3. 若仍不满足时效，再看第二轮：
   - `write.batch.settle.ms`
   - 更稳定的资源池
   - `repartition(24, col(...))` 小范围试验

## 备注

- 这次排查中发现业务日志里打印了 YMatrix 明文密码，后续应尽快移除。
- 若后续要面向客户输出说明，可直接基于本文件整理。
