# Spark Read Projection and COUNT Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `df.count()` execute as one MDB `COUNT(*)` query, preserve distribution keys internally for ordinary projected reads, and ensure all database/RMI failures fail the Spark action instead of returning false zero rows.

**Architecture:** `GreenplumScanBuilder` will select between a one-partition JDBC scalar-count scan and the existing gpfdist scan. The gpfdist scan will use a pure schema planner to separate Spark-visible output columns from internally transferred distribution columns, then project parsed rows back to the output schema. Read startup and EOF handling will distinguish explicit no-work/completion states from protocol failures.

**Tech Stack:** Scala 2.12.17, Spark DataSource V2 3.4.1, PostgreSQL JDBC 42.7.2, Java RMI, Maven, main-style regression tests

---

## File map

- Create `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala`: pure distribution-key matching, transfer-schema construction, and row projection.
- Create `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/CountPushdown.scala`: recognize supported Spark aggregate expressions and describe the scalar result schema.
- Create `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala`: one-partition JDBC execution of pushed `COUNT(*)`.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala`: retain source/output schemas, track unsupported filters, and select aggregate or gpfdist scan.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala`: use transfer schema for external-table SQL and expose driver failures.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumBatch.scala`: pass both schemas and fail before creating readers.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumMicroBatch.scala`: keep streaming reads on the same schema/failure contract.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumPartitionReaderFactory.scala`: serialize output and transfer schemas.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumInputPartitionReader.scala`: parse transfer rows and return projected output rows.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala`: treat registry/check-in loss as failure, not EOF.
- Modify `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`: correctly report unsupported filters.
- Modify `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/GPClient.scala`: allow the scalar reader to close its local JDBC pool.
- Create focused main-style tests under `spark-greenplum-connector/src/test/scala`.

### Task 1: Make filter pushdown reporting trustworthy

**Files:**
- Modify: `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`
- Create: `spark-greenplum-connector/src/test/scala/org/apache/spark/sql/itsumma/gpconnector/FilterPushdownTest.scala`

- [ ] **Step 1: Write the failing filter classification test**

Create a main-style test that checks a supported comparison, an unsupported custom filter, and an `Or` containing an unsupported child:

```scala
package org.apache.spark.sql.itsumma.gpconnector

import org.apache.spark.sql.sources.{Filter, GreaterThan, Or}

object FilterPushdownTest {
  private final case class UnsupportedFilter(name: String) extends Filter

  def main(args: Array[String]): Unit = {
    val util = SparkSchemaUtil("Asia/Shanghai")
    val supported = GreaterThan("amount", BigDecimal(50))
    val unsupported = UnsupportedFilter("x")

    val (where, rejected, accepted) = util.pushFilters(Array(supported, unsupported))
    assert(where.contains("amount"))
    assert(rejected.sameElements(Array(unsupported)))
    assert(accepted.sameElements(Array(supported)))

    val (_, rejectedOr, acceptedOr) =
      util.pushFilters(Array(Or(supported, unsupported)))
    assert(rejectedOr.length == 1)
    assert(acceptedOr.isEmpty)
    println("FILTER_PUSHDOWN_TEST_OK")
  }
}
```

- [ ] **Step 2: Compile and run the test to verify the current code fails**

Run:

```text
mvn -pl spark-greenplum-connector -am test-compile -DskipTests
```

Then run `FilterPushdownTest` with the generated test classpath. Expected before the fix: assertion failure because `unsupportedFilters :+ f` does not mutate the existing array.

- [ ] **Step 3: Replace side-effect-free array appends with recursive compilation**

Add a private `compileFilter(filter: Filter): Option[String]` that returns `None` unless the complete filter tree is supported. Build the public result from accepted `(filter, sql)` pairs:

```scala
def pushFilters(filters: Array[Filter]): (String, Array[Filter], Array[Filter]) = {
  val compiled = filters.map(filter => filter -> compileFilter(filter))
  val unsupported = compiled.collect { case (filter, None) => filter }
  val supported = compiled.collect { case (filter, Some(_)) => filter }
  val where = compiled.collect {
    case (_, Some(sql)) => s"($sql)"
  }.mkString(" AND ")
  (where, unsupported, supported)
}
```

`And`, `Or`, and `Not` must return `Some` only when all children compile. Reuse the existing value/date/timestamp escaping methods for leaf filters.

- [ ] **Step 4: Re-run the focused test**

Expected final line:

```text
FILTER_PUSHDOWN_TEST_OK
```

- [ ] **Step 5: Commit Task 1**

```text
git add spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala spark-greenplum-connector/src/test/scala/org/apache/spark/sql/itsumma/gpconnector/FilterPushdownTest.scala
git commit -m "fix: report unsupported Spark filters correctly"
```

### Task 2: Plan hidden distribution columns and output projection

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala`

- [ ] **Step 1: Write failing tests for schema planning and projection**

Cover:

```scala
val source = StructType(Seq(
  StructField("order_id", LongType),
  StructField("amount", DecimalType(10, 2))
))
val amountOnly = StructType(Seq(source("amount")))

val plan = ReadSchemaPlan.build(
  source, amountOnly, "distributed by (order_id)", "order_id")
assert(plan.outputSchema == amountOnly)
assert(plan.transferSchema.fieldNames.sameElements(Array("amount", "order_id")))
assert(plan.distributionClause == "distributed by (order_id)")

val unresolved = ReadSchemaPlan.build(
  amountOnly, amountOnly, "distributed by (order_id)", "order_id")
assert(unresolved.transferSchema == amountOnly)
assert(unresolved.distributionClause == "distributed randomly")
```

Also test:

- output schema already contains the key;
- multiple distribution keys;
- exact matching for `"CaseSensitiveKey"`;
- empty output schema;
- a parsed transfer row is projected back to the correct output field order.

- [ ] **Step 2: Compile to verify `ReadSchemaPlan` is missing**

Expected: compilation failure naming `ReadSchemaPlan`.

- [ ] **Step 3: Implement immutable planning types**

Create:

```scala
private[gpconnector] final case class ReadSchemaPlan(
  outputSchema: StructType,
  transferSchema: StructType,
  distributionClause: String,
  unresolvedDistributionColumns: Seq[String]
) {
  val projector = new ReadRowProjector(transferSchema, outputSchema)
}
```

`ReadSchemaPlan.build()` must:

1. parse `distributionColNames`;
2. resolve unquoted names case-insensitively and quoted names exactly;
3. append missing resolved fields from `sourceSchema`;
4. preserve random/replicated/empty clauses;
5. use `distributed randomly` only if at least one named key cannot be resolved.

- [ ] **Step 4: Implement the precomputed row projector**

`ReadRowProjector` calculates output indexes once. Its per-row method uses `InternalRow.get(index, dataType)` and returns `GenericInternalRow`; if schemas are identical it may return the original row. For an empty output schema it returns a zero-field row.

- [ ] **Step 5: Run the focused test**

Expected final line:

```text
READ_SCHEMA_PLAN_TEST_OK
```

- [ ] **Step 6: Commit Task 2**

```text
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/ReadSchemaPlan.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/ReadSchemaPlanTest.scala
git commit -m "feat: preserve distribution keys in internal read schema"
```

### Task 3: Recognize safe complete COUNT pushdown

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/CountPushdown.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/CountPushdownTest.scala`

- [ ] **Step 1: Write the failing aggregate-recognition test**

Construct Spark 3.4.1 `Aggregation` instances and assert:

```scala
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, CountStar, Max}

val count = new Aggregation(
  Array[AggregateFunc](new CountStar()),
  Array.empty[Expression])
val groupedCount = new Aggregation(
  Array[AggregateFunc](new CountStar()),
  Array[Expression](Expressions.column("order_id")))
val nonCount = new Aggregation(
  Array[AggregateFunc](new Max(Expressions.column("amount"))),
  Array.empty[Expression])

assert(CountPushdown.accept(count, allFiltersPushed = true, sqlTransfer = "").nonEmpty)
assert(CountPushdown.accept(count, allFiltersPushed = false, sqlTransfer = "").isEmpty)
assert(CountPushdown.accept(groupedCount, allFiltersPushed = true, sqlTransfer = "").isEmpty)
assert(CountPushdown.accept(nonCount, allFiltersPushed = true, sqlTransfer = "").isEmpty)
assert(CountPushdown.outputSchema.fields.head.dataType == LongType)
```

- [ ] **Step 2: Compile to verify the helper is missing**

Expected: compilation failure naming `CountPushdown`.

- [ ] **Step 3: Implement CountStar-only recognition**

Create a pure helper that accepts exactly one `CountStar`, no grouping expressions, no rejected filters, and an empty custom `sqlTransfer`. Return a `CountPushdown` value with:

```scala
val outputSchema = StructType(Seq(
  StructField("connector_count", LongType, nullable = false)
))
```

- [ ] **Step 4: Run the focused test**

Expected final line:

```text
COUNT_PUSHDOWN_TEST_OK
```

- [ ] **Step 5: Commit Task 3**

```text
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/CountPushdown.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/CountPushdownTest.scala
git commit -m "feat: recognize complete count pushdown"
```

### Task 4: Add a one-partition JDBC scalar-count scan

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/GPClient.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumCountScanTest.scala`

- [ ] **Step 1: Write failing SQL and partition tests**

Extract count SQL construction into a testable method. Verify:

```scala
assert(CountSql.build("cdm_dwyz.orders", "amount > 50", "cdm_dwyz") ==
  "select count(*)::bigint as connector_count from cdm_dwyz.orders where amount > 50")
assert(CountSql.build("select * from orders", "", "public") ==
  "select count(*)::bigint as connector_count from (select * from orders) connector_count_source")
assert(new GreenplumCountScan(options, "", CountPushdown.outputSchema)
  .toBatch.planInputPartitions().length == 1)
```

Use a JDBC proxy in the reader test to verify one returned `Long` produces one `InternalRow` and a `SQLException` escapes rather than becoming EOF.

- [ ] **Step 2: Compile to verify the scan classes are missing**

Expected: compilation failures naming `CountSql` and `GreenplumCountScan`.

- [ ] **Step 3: Add explicit GPClient pool cleanup**

Add:

```scala
def close(): Unit = this.synchronized {
  if (pool != null) {
    pool.close()
    pool = null
  }
}
```

The scalar reader calls this after closing its result set, statement, and connection.

- [ ] **Step 4: Implement count scan, batch, partition, factory, and reader**

The scan:

- returns `CountPushdown.outputSchema`;
- implements `Batch` directly;
- plans exactly one `InputPartition`;
- creates one serializable reader factory.

The executor reader:

1. creates `GPClient(optionsFactory)`;
2. opens one JDBC connection;
3. resolves the default MDB schema;
4. builds table or wrapped-query SQL;
5. appends the fully pushed `whereClause`;
6. executes once;
7. returns one `GenericInternalRow(Array[Any](count))`;
8. throws on missing/multiple-invalid results or JDBC failures;
9. closes all resources deterministically.

- [ ] **Step 5: Run the focused test**

Expected final line:

```text
GREENPLUM_COUNT_SCAN_TEST_OK
```

- [ ] **Step 6: Commit Task 4**

```text
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/GPClient.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumCountScan.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumCountScanTest.scala
git commit -m "feat: execute pushed counts through one JDBC partition"
```

### Task 5: Select COUNT or gpfdist scan in GreenplumScanBuilder

**Files:**
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilderTest.scala`

- [ ] **Step 1: Write a failing builder-state test**

Verify that:

- source schema remains unchanged after `pruneColumns`;
- output schema changes;
- safe `CountStar` creates `GreenplumCountScan`;
- an unsupported filter or grouped aggregate creates ordinary `GreenplumScan`.

- [ ] **Step 2: Compile and run to observe the missing aggregate interface**

Expected: failure because the builder does not implement `SupportsPushDownAggregates`.

- [ ] **Step 3: Implement aggregate state and scan selection**

Change the builder to:

```scala
class GreenplumScanBuilder(
  optionsFactory: GPOptionsFactory,
  rowSet: GreenplumRowSet,
  sourceSchema: StructType)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
```

Maintain:

```scala
private var outputSchema = sourceSchema
private var unsupportedFilters = Array.empty[Filter]
private var countPushdown = Option.empty[CountPushdown]
```

`pushAggregation()` accepts only through `CountPushdown.accept()`. `supportCompletePushDown()` returns true only for the accepted plan. `build()` returns `GreenplumCountScan` or `GreenplumScan(options, rowSet, sourceSchema, outputSchema, whereClause)`.

- [ ] **Step 4: Run the builder test**

Expected final line:

```text
GREENPLUM_SCAN_BUILDER_TEST_OK
```

- [ ] **Step 5: Commit Task 5**

```text
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilder.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumScanBuilderTest.scala
git commit -m "feat: route count actions to aggregate scan"
```

### Task 6: Use transfer schema throughout the gpfdist read path

**Files:**
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumBatch.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumMicroBatch.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumPartitionReaderFactory.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumInputPartitionReader.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumInputProjectionTest.scala`

- [ ] **Step 1: Write a failing reader projection test**

Create transfer schema `(amount, order_id)`, output schema `(amount)`, feed one tab-delimited row, and assert the reader-side conversion returns one-field output. Also verify zero-field output still returns one row per input record.

- [ ] **Step 2: Compile to expose constructor and schema mismatches**

Expected: failures because the read path accepts only one schema.

- [ ] **Step 3: Build `ReadSchemaPlan` after detecting target distribution**

Change `GreenplumScan` constructor to accept source and output schemas. After `getTableDistributionPolicy()`:

```scala
private val schemaPlan = ReadSchemaPlan.build(
  sourceSchema,
  outputSchema,
  distributedByClause,
  distributionColNames)
private val transferSchema = schemaPlan.transferSchema
```

Log a warning containing unresolved keys before using the random fallback.

- [ ] **Step 4: Use the correct schema at each boundary**

- `readSchema()` returns `outputSchema`;
- external-table columns and `INSERT ... SELECT` use `transferSchema`;
- batch/microbatch/factory carry both schemas;
- reader parses text using `transferSchema`;
- reader applies the precomputed projector and returns `outputSchema`;
- placeholder schema is used only when both output and transfer schemas are empty.

- [ ] **Step 5: Run projection and schema-plan tests**

Expected final lines:

```text
READ_SCHEMA_PLAN_TEST_OK
GREENPLUM_INPUT_PROJECTION_TEST_OK
```

- [ ] **Step 6: Commit Task 6**

```text
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/reader/GreenplumInputProjectionTest.scala
git commit -m "fix: keep projected gpfdist reads distribution-safe"
```

### Task 7: Make protocol failures impossible to interpret as EOF

**Files:**
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumBatch.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumMicroBatch.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumInputPartitionReader.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/ReadStartupStateTest.scala`

- [ ] **Step 1: Write failing state tests**

Extract a small `ReadStartupState` decision helper and test:

```scala
assert(ReadStartupState.connected.hasRowsOrCanRead)
assert(ReadStartupState.noWork.isNormalEof)
assert(expectFailure {
  ReadStartupState.failed("original MDB error").throwIfReadable()
}.getMessage.contains("original MDB error"))
assert(expectFailure {
  ReadStartupState.disconnected.throwIfReadable()
}.getMessage.contains("not connected"))

private def expectFailure(body: => Unit): Exception = {
  try {
    body
    throw new AssertionError("Expected read startup failure")
  } catch {
    case e: Exception => e
  }
}
```

Also extend existing transfer tests so the first MDB reason survives a later cleanup failure.

- [ ] **Step 2: Run tests to verify the missing distinction**

Expected: failure because current code has only `connected=false`, which represents both no-work and connection loss.

- [ ] **Step 3: Stop swallowing registry and check-in errors**

In `RMISlave`:

- registry lookup failure throws for reads and writes;
- `NoSuchObjectException` during check-in throws;
- successful Master response with no segment assignment sets explicit `noWork=true`;
- abort reason remains in `TransferAbortState`.

Represent the decision with a small immutable type:

```scala
private[gpconnector] final case class ReadStartupState(
  hasRowsOrCanRead: Boolean,
  isNormalEof: Boolean,
  failureMessage: Option[String]) {

  def throwIfReadable(): Unit =
    failureMessage.foreach(message => throw new IllegalStateException(message))
}

private[gpconnector] object ReadStartupState {
  val connected = ReadStartupState(
    hasRowsOrCanRead = true, isNormalEof = false, failureMessage = None)
  val noWork = ReadStartupState(
    hasRowsOrCanRead = false, isNormalEof = true, failureMessage = None)
  val disconnected = failed("read coordinator is not connected")
  def failed(message: String): ReadStartupState =
    ReadStartupState(
      hasRowsOrCanRead = false,
      isNormalEof = false,
      failureMessage = Some(message))
}
```

- [ ] **Step 4: Add driver failure gates**

Expose:

```scala
private[reader] def throwIfSqlFailed(): Unit = {
  val failure = sqlFailure.get()
  if (failure != null) {
    throw new IllegalStateException(TransferFailure.message(failure), failure)
  }
}
```

Call it before and after planning waits and before constructing reader factories.

- [ ] **Step 5: Tighten reader EOF and cleanup behavior**

`GreenplumInputPartitionReader.next()` returns `false` only for:

- explicit no-work;
- normal transfer completion after all buffered rows are drained.

Disconnected, aborted, registry-lost and premature-completion states throw. `closeInternal()` records a non-benign failure, completes resource cleanup, then rethrows it.

- [ ] **Step 6: Run failure-focused tests**

Expected:

```text
READ_STARTUP_STATE_TEST_OK
TRANSFER_ABORT_STATE_TEST_OK
TRANSFER_FAILURE_TEST_OK
```

- [ ] **Step 7: Commit Task 7**

```text
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi
git commit -m "fix: fail Spark reads on coordinator loss"
```

### Task 8: Full regression, package inspection, and local handoff

**Files:**
- Verify all modified source and tests.
- Preserve the user's existing status files and unrelated working-tree content.

- [ ] **Step 1: Run source consistency checks**

```text
git diff --check 9167269..HEAD
git status --short
```

Expected: no whitespace errors; pre-existing `status.md`, `todo.md`, manuals and older JARs remain untouched by feature commits.

- [ ] **Step 2: Compile all main and test sources**

```text
mvn -pl spark-greenplum-connector -am clean test-compile -DskipTests
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 3: Run every main-style regression**

Run:

```text
FilterPushdownTest
ReadSchemaPlanTest
CountPushdownTest
GreenplumCountScanTest
GreenplumScanBuilderTest
GreenplumInputProjectionTest
ReadStartupStateTest
TransferAbortStateTest
TransferFailureTest
SparkSchemaLookupTest
ServerPublishMappingTest
```

Each process must exit zero and print its `_TEST_OK` marker.

- [ ] **Step 4: Package the connector locally if a JDK and Maven are available**

```text
mvn -pl spark-greenplum-connector -am clean package -DskipTests
```

Expected: `BUILD SUCCESS` and a readable shaded connector JAR under `spark-greenplum-connector/target`.

- [ ] **Step 5: Inspect the JAR**

Verify it contains:

```text
com/itsumma/gpconnector/reader/GreenplumCountScan.class
com/itsumma/gpconnector/reader/ReadSchemaPlan.class
com/itsumma/gpconnector/reader/GreenplumScan$SqlThread.class
com/itsumma/gpconnector/rmi/RMISlave.class
META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
```

- [ ] **Step 6: Verify the physical plan in an available Spark/MDB test environment**

Run the original filtered `df.count()` and require:

- Spark physical plan shows a pushed aggregate scan;
- MDB receives one `SELECT count(*)::bigint ... WHERE amount > 50`;
- no writable external table is created for the count;
- result matches direct MDB SQL;
- an intentionally invalid MDB query makes `count()` throw.

This step is not performed against a remote or customer environment unless the user explicitly authorizes that environment.

- [ ] **Step 7: Final handoff**

Report:

- commits;
- tests and build results;
- local JAR path and SHA-256 if packaging succeeded;
- whether live MDB validation was performed;
- any remaining verification that requires explicit remote authorization.
