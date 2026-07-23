# Duplicate Output Columns Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 identity 投影中的重复同名列按字段位置保留各自的值，同时保持单 `InternalRow` 优化和非 identity 映射逻辑不变。

**Architecture:** `ReadRowProjector` 在 transfer/output schema 完全相等时直接生成位置索引；只有 schema 不同时才执行既有名称查找。回归测试使用两个同名 Long 字段证明第二个值不会再复用第一个字段。

**Tech Stack:** Scala 2.12、Spark Catalyst `InternalRow`、Spark SQL `StructType`、Maven

---

### Task 1: 增加重复列 identity 回归测试

**Files:**
- Modify: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala`

- [ ] **Step 1: 写失败测试**

在 `ReadSchemaPlanTest.main` 的普通投影断言后加入：

```scala
val duplicateSchema = StructType(Seq(
  StructField("x", LongType),
  StructField("x", LongType)))
val duplicateProjector =
  new ReadRowProjector(duplicateSchema, duplicateSchema)
val duplicateRow = duplicateProjector.projectText(
  schemaUtil,
  Array("11", "22"))
assert(duplicateRow.getLong(0) == 11L)
assert(duplicateRow.getLong(1) == 22L)
```

- [ ] **Step 2: 运行测试确认旧实现失败**

在隔离构建目录运行：

```bash
mvn -pl spark-greenplum-connector -am test-compile
cd spark-greenplum-connector
mvn -q dependency:build-classpath \
  -Dmdep.outputFile=target/test-classpath.txt
cpv="target/test-classes:target/classes:$(cat target/test-classpath.txt)"
java -cp "$cpv" com.itsumma.gpconnector.reader.ReadSchemaPlanTest
```

Expected: 第二个断言失败，因为旧索引为 `[0, 0]`，实际第二列是 `11L`。

### Task 2: identity schema 使用位置索引

**Files:**
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala`
- Test: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala`

- [ ] **Step 1: 提取既有名称解析函数**

在 `ReadRowProjector` 中加入：

```scala
private def resolveOutputIndex(outputField: StructField): Int = {
  val exactIndex =
    transferSchema.fields.indexWhere(_.name == outputField.name)
  val index =
    if (exactIndex >= 0) exactIndex
    else transferSchema.fields.indexWhere(
      _.name.equalsIgnoreCase(outputField.name))
  require(
    index >= 0,
    s"Output column ${outputField.name} is absent from transfer schema")
  index
}
```

同时把类型导入改为：

```scala
import org.apache.spark.sql.types.{StructField, StructType}
```

- [ ] **Step 2: identity 使用位置索引，非 identity 保留名称映射**

将 `outputIndexes` 改为：

```scala
private val outputIndexes: Array[Int] =
  if (transferSchema == outputSchema) {
    outputSchema.fields.indices.toArray
  } else {
    outputSchema.fields.map(resolveOutputIndex)
  }
```

`projectText()` 不做其他修改，因此仍然直接构造一个最终 `SpecificInternalRow`。

- [ ] **Step 3: 运行纯回归测试**

```bash
mvn -pl spark-greenplum-connector -am clean test-compile
cd spark-greenplum-connector
mvn -q dependency:build-classpath \
  -Dmdep.outputFile=target/test-classpath.txt
cpv="target/test-classes:target/classes:$(cat target/test-classpath.txt)"
java -cp "$cpv" com.itsumma.gpconnector.reader.ReadSchemaPlanTest
```

Expected:

```text
READ_SCHEMA_PLAN_TEST_OK
```

- [ ] **Step 4: 静态检查并提交**

```bash
git diff --check
git add \
  spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala \
  spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala
git commit -m "fix: preserve duplicate identity columns"
```

### Task 3: 143 隔离构建与产物回收

**Files:**
- Verify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala`
- Verify: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala`
- Create artifact: `spark-ymatrix-connector_2.12-3.1_20260723_duplicate_fix.jar`

- [ ] **Step 1: 从最终提交创建新的隔离源码目录**

使用以下命令取得提交和目标目录：

```bash
commit="$(git rev-parse --short HEAD)"
remote_dir="/root/spark-greenplum-connector-duplicate-fix-20260723-${commit}"
```

把 `git archive HEAD` 上传并解压到 `$remote_dir`。创建前确认目标不存在，不修改
`/root/spark-greenplum-connector`。

- [ ] **Step 2: 编译、运行纯测试并打包**

```bash
mvn -pl spark-greenplum-connector -am clean test-compile
mvn -pl spark-greenplum-connector -am -DskipTests package
```

运行 `ReadSchemaPlanTest`，预期输出 `READ_SCHEMA_PLAN_TEST_OK`。不连接 MDB。

- [ ] **Step 3: 核对并下载最终 JAR**

确认 JAR 包含 `ReadRowProjector.class`，计算远端 SHA-256，下载为：

```text
spark-ymatrix-connector_2.12-3.1_20260723_duplicate_fix.jar
```

本地 SHA-256 必须与远端一致；不部署到 `/opt/spark/jars`。
