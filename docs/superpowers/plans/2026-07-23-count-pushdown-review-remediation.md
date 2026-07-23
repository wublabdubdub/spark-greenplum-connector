# COUNT Pushdown Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 COUNT 下推执行位置、Spark 聚合回调顺序、RMI 构造失败泄漏和隐藏分布键双行对象四项风险，同时保持普通读取、过滤和失败传播语义不变。

**Architecture:** COUNT SQL 在 Spark Driver 的 `planInputPartitions()` 阶段通过已有 `GreenplumRowSet` 连接执行，Executor 只读取分区携带的 Long 字面量。聚合完整下推判断改为无状态计算；RMI 构造失败统一强制 unexport；文本读取根据预计算索引直接构造输出 `SpecificInternalRow`。

**Tech Stack:** Scala 2.12、Spark DataSource V2、JDBC、Java RMI、Maven、Spark 3.5.3 运行时兼容检查

---

## 文件结构

- 修改 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala`：无状态判断完整聚合下推，并把 `GreenplumRowSet` 交给 COUNT scan。
- 修改 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala`：Driver 执行 COUNT，分区只携带结果值，Executor reader 不再连接数据库。
- 新建 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RmiObjectLifecycle.scala`：集中处理幂等 RMI unexport。
- 修改 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala`：两个构造失败分支先释放 RMI export，再抛出异常。
- 修改 `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`：增加按源字段索引直接解析输出 row 的重载。
- 修改 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala`：投影器直接消费文本字段。
- 修改 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumInputPartitionReader.scala`：每个 reader 复用一个 schema util，并直接生成输出 row。
- 修改三个现有 reader 测试并新建一个 RMI 生命周期测试。

### Task 1: Spark 完整聚合下推回调顺序

**Files:**
- Modify: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilderTest.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala`

- [ ] **Step 1: 先写 Spark 实际调用顺序的失败测试**

把 COUNT 测试顺序改为先查询完整下推能力，再实际推送：

```scala
assert(builder.supportCompletePushDown(count))
assert(!builder.hasCountPushdown)
assert(builder.pushAggregation(count))
assert(builder.hasCountPushdown)
assert(builder.build().isInstanceOf[GreenplumCountScan])
```

同时对含未支持过滤器的 builder 验证两个接口均返回 false：

```scala
assert(!unsupportedBuilder.supportCompletePushDown(count))
assert(!unsupportedBuilder.pushAggregation(count))
```

- [ ] **Step 2: 编译测试并确认旧实现失败**

Run:

```bash
mvn -pl spark-greenplum-connector -am test-compile
```

Expected: 编译成功；运行 `GreenplumScanBuilderTest` 时在 complete-before-push 断言失败。

- [ ] **Step 3: 把完整下推判断改成无状态**

在 `GreenplumScanBuilder.scala` 中使用：

```scala
override def supportCompletePushDown(
    aggregation: Aggregation): Boolean =
  CountPushdown.accept(
    aggregation,
    allFiltersPushed = unsupportedFilters.isEmpty,
    sqlTransfer = optionsFactory.sqlTransfer).nonEmpty
```

- [ ] **Step 4: 运行测试并提交**

Expected: `GREENPLUM_SCAN_BUILDER_TEST_OK`。

```bash
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilderTest.scala
git commit -m "fix: make complete count pushdown stateless"
```

### Task 2: COUNT 改为 Driver 执行

**Files:**
- Modify: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumCountScanTest.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala`

- [ ] **Step 1: 写 Driver 查询、lazy 计划和字面量 reader 的失败测试**

测试使用 JDBC 动态代理，覆盖：

```scala
assert(DriverCountQuery.execute(
  connectionReturning(statement),
  "cdm_dwyz.orders",
  "amount > 50",
  "cdm_dwyz") == 123L)

val loads = new AtomicInteger(0)
val scan = new GreenplumCountScan(
  options,
  null,
  "cdm_dwyz.orders",
  "amount > 50",
  () => { loads.incrementAndGet(); 123L })
val first = scan.planInputPartitions()
val second = scan.planInputPartitions()
assert(loads.get() == 1)
assert(first.head.asInstanceOf[GreenplumCountInputPartition].countValue == 123L)
assert(second.head.asInstanceOf[GreenplumCountInputPartition].countValue == 123L)

val reader = GreenplumCountReaderFactory.createReader(first.head)
assert(reader.next())
assert(reader.get().getLong(0) == 123L)
assert(!reader.next())
reader.close()
```

再分别让 `executeQuery()` 抛 `SQLException`、让 `ResultSet.next()` 返回 false、让 `wasNull()` 返回 true，断言异常不会被改写成 0。

- [ ] **Step 2: 编译并确认新 API 尚不存在**

Run:

```bash
mvn -pl spark-greenplum-connector -am test-compile
```

Expected: FAIL，提示 `DriverCountQuery` 或新构造参数不存在。

- [ ] **Step 3: 实现 Driver COUNT 查询单元**

在 `GreenplumCountScan.scala` 增加：

```scala
private[gpconnector] object DriverCountQuery {
  def execute(
      connection: Connection,
      tableOrQuery: String,
      whereClause: String,
      defaultSchema: String): Long = {
    val sql = CountSql.build(tableOrQuery, whereClause, defaultSchema)
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      statement = connection.prepareStatement(sql)
      resultSet = statement.executeQuery()
      if (!resultSet.next())
        throw new IllegalStateException(s"COUNT query returned no row: $sql")
      val count = resultSet.getLong(1)
      if (resultSet.wasNull())
        throw new IllegalStateException(s"COUNT query returned NULL: $sql")
      count
    } finally {
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
    }
  }

  def execute(
      optionsFactory: GPOptionsFactory,
      rowSet: GreenplumRowSet,
      tableOrQuery: String,
      whereClause: String): Long = {
    val connection = rowSet.getGpClient.getConnection()
    try {
      val schema =
        GPClient.checkDbObjSearchPath(connection, optionsFactory.dbSchema)
      execute(connection, tableOrQuery, whereClause, schema)
    } finally {
      connection.close()
    }
  }
}
```

实际实现的 finally 必须保留第一个异常，并把后续关闭异常放入 suppressed，避免关闭错误覆盖 SQL 根因。

- [ ] **Step 4: 让 scan 在 Driver 生成携带 Long 的唯一分区**

实现以下接口：

```scala
private[gpconnector] final class GreenplumCountScan(
    optionsFactory: GPOptionsFactory,
    rowSet: GreenplumRowSet,
    tableOrQuery: String,
    whereClause: String,
    suppliedCountLoader: () => Long = null)
  extends Scan with Batch {

  private lazy val countValue: Long =
    if (suppliedCountLoader != null) suppliedCountLoader()
    else DriverCountQuery.execute(
      optionsFactory, rowSet, tableOrQuery, whereClause)

  override def readSchema(): StructType = CountPushdown.OutputSchema
  override def toBatch: Batch = this
  override def planInputPartitions(): Array[InputPartition] =
    Array(GreenplumCountInputPartition(countValue))
  override def createReaderFactory(): PartitionReaderFactory =
    GreenplumCountReaderFactory
}

private[gpconnector] final case class GreenplumCountInputPartition(
    countValue: Long) extends InputPartition
```

`GreenplumCountReaderFactory` 改成无参数 object；reader 只保存 Long 和一次性游标状态，禁止保存 `GPOptionsFactory`、JDBC URL、Connection 或 SQL。

- [ ] **Step 5: builder 传入已有 rowSet**

```scala
new GreenplumCountScan(
  optionsFactory,
  rowSet,
  optionsFactory.tableOrQuery,
  whereClause)
```

- [ ] **Step 6: 运行 COUNT 与 builder 测试并提交**

Expected:

```text
GREENPLUM_COUNT_SCAN_TEST_OK
GREENPLUM_SCAN_BUILDER_TEST_OK
```

```bash
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumCountScanTest.scala
git commit -m "fix: execute pushed counts on Spark driver"
```

### Task 3: RMI 构造失败立即 unexport

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RmiObjectLifecycle.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/RmiObjectLifecycleTest.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala`

- [ ] **Step 1: 写幂等清理失败测试**

```scala
package com.itsumma.gpconnector.rmi

import java.rmi.Remote
import java.rmi.server.UnicastRemoteObject

object RmiObjectLifecycleTest {
  private trait Ping extends Remote
  private final class PingImpl extends UnicastRemoteObject with Ping

  def main(args: Array[String]): Unit = {
    val remote = new PingImpl
    assert(RmiObjectLifecycle.forceUnexport(remote))
    assert(!RmiObjectLifecycle.forceUnexport(remote))
    println("RMI_OBJECT_LIFECYCLE_TEST_OK")
  }
}
```

- [ ] **Step 2: 编译并确认 helper 尚不存在**

Run:

```bash
mvn -pl spark-greenplum-connector -am test-compile
```

Expected: FAIL，提示 `RmiObjectLifecycle` 不存在。

- [ ] **Step 3: 实现幂等 helper**

```scala
package com.itsumma.gpconnector.rmi

import java.rmi.{NoSuchObjectException, Remote}
import java.rmi.server.UnicastRemoteObject

private[gpconnector] object RmiObjectLifecycle {
  def forceUnexport(remote: Remote): Boolean =
    try {
      UnicastRemoteObject.unexportObject(remote, true)
    } catch {
      case _: NoSuchObjectException => false
    }
}
```

- [ ] **Step 4: 在 RMISlave 两个构造失败分支调用清理**

类内加入：

```scala
private def unexportAfterStartupFailure(failure: Throwable): Unit = {
  try RmiObjectLifecycle.forceUnexport(this)
  catch {
    case cleanupFailure: Throwable =>
      failure.addSuppressed(cleanupFailure)
  }
}
```

registry lookup 分支先构造 `IllegalStateException`，再清理并抛出：

```scala
val startupFailure = new IllegalStateException(msg, e)
unexportAfterStartupFailure(startupFailure)
throw startupFailure
```

check-in 分支使用方向正确的消息：

```scala
val direction = if (readOrWrite) "read" else "write"
val message =
  s"Unable to check in $direction instance $instanceId for query $queryId: " +
    s"${failure.getClass.getCanonicalName}: ${failure.getMessage}"
val startupFailure = new IllegalStateException(message, failure)
unexportAfterStartupFailure(startupFailure)
throw startupFailure
```

- [ ] **Step 5: 运行测试并提交**

Expected: `RMI_OBJECT_LIFECYCLE_TEST_OK`。

```bash
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RmiObjectLifecycle.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/RmiObjectLifecycleTest.scala
git commit -m "fix: unexport failed RMI readers"
```

### Task 4: 隐藏分布键读取只创建一个 row

**Files:**
- Modify: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala`
- Modify: `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumInputPartitionReader.scala`

- [ ] **Step 1: 把投影测试改成直接输入文本字段**

```scala
val schemaUtil = SparkSchemaUtil("Asia/Shanghai")
val projected =
  plan.projector.projectText(schemaUtil, Array("12.34", "42"))
assert(projected.numFields == 1)
assert(projected.getDecimal(0, 10, 2).toString == "12.34")

val reorderedOutput =
  StructType(Seq(source("CaseSensitiveKey"), source("amount")))
val reordered = ReadSchemaPlan.build(
  source,
  reorderedOutput,
  """distributed by ("CaseSensitiveKey")""",
  "\"CaseSensitiveKey\"")
val reorderedRow =
  reordered.projector.projectText(schemaUtil, Array("hello", "12.34"))
assert(reorderedRow.getUTF8String(0).toString == "hello")
assert(reorderedRow.getDecimal(1, 10, 2).toString == "12.34")

val emptyRow =
  emptyOutput.projector.projectText(schemaUtil, Array("42"))
assert(emptyRow.numFields == 0)
```

加入 NULL 和错误字段数断言，确保解析错误继续抛出。

- [ ] **Step 2: 编译并确认 `projectText` 尚不存在**

Run:

```bash
mvn -pl spark-greenplum-connector -am test-compile
```

Expected: FAIL，提示 `projectText` 不存在。

- [ ] **Step 3: 为 SparkSchemaUtil 增加索引解析重载**

旧入口委托 identity 索引：

```scala
def textToInternalRow(
    schema: StructType,
    fields: Array[String]): InternalRow =
  textToInternalRow(schema, fields, schema.fields.indices.toArray)
```

新入口验证索引数量和边界，再只遍历 output schema：

```scala
def textToInternalRow(
    schema: StructType,
    fields: Array[String],
    sourceIndexes: Array[Int]): InternalRow = {
  if (schema.fields.length != sourceIndexes.length)
    throw new SQLException(
      s"textToInternalRow: schema.size=${schema.fields.length}, " +
        s"but ${sourceIndexes.length} source indexes received")
  sourceIndexes.foreach { sourceIndex =>
    if (sourceIndex < 0 || sourceIndex >= fields.length)
      throw new SQLException(
        s"textToInternalRow: source index $sourceIndex is outside " +
          s"${fields.length} data columns")
  }
  val row = new SpecificInternalRow(schema.fields.map(_.dataType))
  sourceIndexes.zipWithIndex.foreach {
    case (sourceIndex, outputIndex) =>
      val txt = fields(sourceIndex)
      // 按 schema.fields(outputIndex).dataType 执行原有完整类型转换，
      // 并把时间戳报错中的列号改为 outputIndex。
  }
  row
}
```

实现时原样保留 String、数值、Timestamp、Date、Boolean、Decimal、Binary 的全部转换分支和 NULL 语义；唯一变化是 `txt` 来自 `fields(sourceIndex)`。

- [ ] **Step 4: projector 直接解析 output row**

移除 `GenericInternalRow` 和 transfer row 投影：

```scala
private[gpconnector] final class ReadRowProjector(
    transferSchema: StructType,
    outputSchema: StructType) extends Serializable {

  private val outputIndexes: Array[Int] = outputSchema.fields.map { outputField =>
    val exactIndex = transferSchema.fields.indexWhere(_.name == outputField.name)
    val index =
      if (exactIndex >= 0) exactIndex
      else transferSchema.fields.indexWhere(
        _.name.equalsIgnoreCase(outputField.name))
    require(index >= 0,
      s"Output column ${outputField.name} is absent from transfer schema")
    index
  }

  def projectText(
      schemaUtil: SparkSchemaUtil,
      fields: Array[String]): InternalRow = {
    if (transferSchema.length != fields.length)
      throw new SQLException(
        s"ReadRowProjector: transfer schema.size=${transferSchema.length}, " +
          s"but ${fields.length} data columns received")
    schemaUtil.textToInternalRow(outputSchema, fields, outputIndexes)
  }
}
```

- [ ] **Step 5: 生产读取路径复用 schema util 并调用 projectText**

在 reader 字段区创建一次：

```scala
private val schemaUtil = SparkSchemaUtil(optionsFactory.dbTimezone)
```

解析区改为：

```scala
val row = progressTracker.trackProgress("parseFields") {
  rowProjector.projectText(schemaUtil, fields)
}
```

- [ ] **Step 6: 运行测试并提交**

Expected: `READ_SCHEMA_PLAN_TEST_OK`。

```bash
git add spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumInputPartitionReader.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala
git commit -m "perf: project read rows without transfer row"
```

### Task 5: 全量纯边界回归与 143 隔离构建

**Files:**
- Verify: `spark-greenplum-connector/src/main/scala`
- Verify: `spark-greenplum-connector/src/test/scala`
- Create artifact: `spark-ymatrix-connector_2.12-3.1_20260723_review_fixes.jar`

- [ ] **Step 1: 静态检查四项修复边界**

Run:

```bash
rg -n "new GPClient|PreparedStatement|Connection" spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala
rg -n "countPushdown.nonEmpty &&" spark-greenplum-connector/src/main/scala
rg -n "project\\(transferRow\\)|new GenericInternalRow" spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader
git diff --check
```

Expected: JDBC 只在 `DriverCountQuery`；其余三个风险模式无命中；`git diff --check` 无输出。

- [ ] **Step 2: 在 143 新建不覆盖原目录的隔离源码目录**

将最终 `git archive HEAD` 上传到：

```text
/root/spark-greenplum-connector-review-fixes-20260723-<short-commit>
```

创建前确认该绝对路径不存在；不得修改 `/root/spark-greenplum-connector`。

- [ ] **Step 3: 编译全部 main/test 源码**

Run on 143:

```bash
mvn -pl spark-greenplum-connector -am clean test-compile
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 4: 执行全部无数据库 main-object 测试**

使用 Maven dependency classpath 加模块 `target/test-classes`、`target/classes`，逐个运行 11 个测试 object。

Expected: 每个 object 输出自身 `_TEST_OK`，并且没有未捕获异常。

- [ ] **Step 5: 打包并核对 JAR 内容**

Run on 143:

```bash
mvn -pl spark-greenplum-connector -am -DskipTests package
jar tf spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
sha256sum spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

Expected: `BUILD SUCCESS`；JAR 包含 `DriverCountQuery`、`GreenplumCountInputPartition`、`RmiObjectLifecycle`、`ReadRowProjector` 类。

- [ ] **Step 6: 回收产物并做最终源码审查**

下载为：

```text
spark-ymatrix-connector_2.12-3.1_20260723_review_fixes.jar
```

对本地文件再次计算 SHA-256，必须与 143 一致。检查 `git status --short`，只报告本次提交和产物，不覆盖、不提交用户原有脏文件；不运行数据库联调，不部署到 `/opt/spark/jars`。
