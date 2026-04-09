# 项目记忆

最后更新：2026-04-03

## 项目目标

本仓库是一个面向 YMatrix/Greenplum 体系的 Spark DataSource V2 Connector。
项目同时支持批读写和微批读取，核心数据传输依赖 `gpfdist`，并通过 driver/executor 之间的 RMI 协调任务与数据通道。

## 仓库结构

- `spark-greenplum-connector/`：主连接器模块
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/`：连接器运行时核心代码
- `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/`：Spark 侧配置、Schema 和工具类
- `test-app/`：本地测试、联调和探针应用
- `examples/`：示例脚本
- `docs/`：操作手册、迁移说明和中文开发文档

## 关键代码区域

- 写路径：
  `writer/GreenplumBatchWrite.scala`、
  `writer/GreenplumDataWriter.scala`、
  `rmi/RMIMaster.scala`、
  `rmi/RMISlave.scala`
- 读路径：
  `reader/GreenplumScan.scala`、
  `reader/GreenplumInputPartitionReader.scala`
- 公共配置与 Schema 工具：
  `org/apache/spark/sql/itsumma/gpconnector/GPOptionsFactory.scala`、
  `org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`

## 协作约定

- 每一次代码或文档修改都必须记录到 `status.md`。
- 任何新增待办、风险或后续动作都必须同步更新 `todo.md`。
- 如果修改影响默认行为、排障方式或架构理解，必须同步更新 `project.md` 和/或 `skiils.md`。
- 不得覆盖工作区中与当前任务无关的已有用户改动。

## 当前技术方向

- 去掉写路径中的固定启动等待。
- 用短静默窗口替代基于 executor 猜测值的 batch 收敛策略。
- 提高高分区写入时的容错能力，让新 host 上的 satellite task 可以挂靠已有 provider。
- 将失败现场扩展为“分区级 dump + gpfdist 流级 dump”，方便定位半行和链路中断。
- 将性能优化重点前移到 Spark 写前准备阶段，优先避免“空判断触发整轮 shuffle”。
- 结合真实客户日志持续补充复盘文档，当前专题记录见 `docs/xiaomi-2026-04-03-ymatrix同步复盘.md`。
- 保持项目记忆文件随每次修改持续更新。

## 常用命令

- 编译主模块：
  `mvn -pl spark-greenplum-connector -DskipTests compile`
- 从仓库根目录打包：
  `mvn clean package`
- 搜索代码：
  `rg "pattern" spark-greenplum-connector/src/main/scala`

## 变更登记规则

后续每次修改完成后，至少执行以下动作：

1. 在 `status.md` 追加一条带日期的变更记录。
2. 在 `todo.md` 关闭已完成项或补充新的后续动作。
3. 如有必要，刷新本文件中的架构说明和工作约定。
