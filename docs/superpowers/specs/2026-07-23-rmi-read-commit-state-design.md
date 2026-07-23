# RMI 读取提交状态修复设计

## 背景

Spark 通过 gpfdist 完成明细读取后，RMIMaster 会广播
`sqlTransferComplete`。RMISlave 收到正常完成通知时设置
`coordinatorSqlComplete=true`，待 gpfdist 数据全部接收和缓冲区排空后再设置
`sqlTransferComplete=true`。

当前 `RMISlave.commit()` 在缓冲区等待结束后，只要
`coordinatorSqlComplete=true` 就抛出“gpfdist stream interrupted”异常。2026-07-23
新增的读取失败传播逻辑不再吞掉该异常，因此正常完成的读取也会被 Spark 判为失败。

## 目标

- 正常完成的 gpfdist 读取不得在 `commit()` 阶段报传输中断。
- 真正的作业中止和协调器提前结束仍须立即失败。
- 两处提交状态检查必须复用同一判定，避免后续行为分叉。
- 保留现有 MDB 根错误传播、任务快速失败和 RMI 清理行为。
- 在 172.16.100.143 完成编译、回归测试和新 JAR 构建。

## 非目标

- 不重写完整 RMI 生命周期。
- 不修改写入协议、端口映射、超时参数或 Spark 配置。
- 不部署新 JAR 到现场 Spark 集群。
- 不执行现场 MDB 业务 SQL。

## 方案

在 `com.itsumma.gpconnector.rmi` 包中增加一个无外部依赖的提交状态判定单元。
它接收 `jobAborted`、`coordinatorSqlComplete` 和
`sqlTransferComplete` 三个布尔状态，并统一判断当前提交是否应失败。

判定规则如下：

| jobAborted | coordinatorSqlComplete | sqlTransferComplete | 结果 |
|---|---|---|---|
| true | 任意 | 任意 | 失败，作业已中止 |
| false | true | false | 失败，协调器已结束但 gpfdist 未完成 |
| false | true | true | 正常完成 |
| false | false | 任意 | 不在该检查点判定失败，继续原有流程 |

`RMISlave.commit()` 在缓冲区排空后的检查和最终传输检查中调用同一判定单元。
原有等待、缓冲区刷新、RMI commit、资源清理和根错误传播逻辑保持不变。

## 错误处理

状态判定只负责区分正常完成与异常完成，不覆盖
`TransferAbortState` 已保存的首个 MDB 根错误。`jobAbort=true` 时仍走现有
fail-fast 路径；协调器完成而 gpfdist 未完成时继续报告传输未完成。

正常完成必须同时满足协调器已完成且 gpfdist 传输已完成。不能通过重新吞掉
`close()` 异常来规避失败，因为那会再次隐藏真正的 MDB 或传输错误。

## 测试

新增纯状态回归测试，至少覆盖：

1. 协调器和 gpfdist 都完成时不失败。
2. 作业中止时失败。
3. 协调器完成但 gpfdist 未完成时失败。
4. 协调器尚未完成时不提前判定失败。

在 143 上先运行新测试验证修复，再运行当前所有 main-style 回归测试，最后执行
`mvn clean package`。构建成功后下载 shaded JAR，使用带时分秒的文件名避免覆盖
已有 `20260723.jar`，并核对文件大小与 SHA-256。

## 验收标准

- 新状态测试全部通过。
- 现有读取失败传播、COUNT 下推、投影、RMI 生命周期和端口映射测试无回归。
- 143 Maven 构建输出 `BUILD SUCCESS`。
- 新 JAR 下载到本地工作区，并提供绝对路径、文件大小和 SHA-256。
