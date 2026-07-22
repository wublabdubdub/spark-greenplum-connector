# Spark Read Failure Fast Propagation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make MDB read failures stop Spark reader tasks immediately and preserve the original database error, while failing missing ordinary tables during temporary-view binding.

**Architecture:** Add a serializable failure reason to the existing RMI control block and a small executor-side abort state that preserves the first reason. The driver SQL thread converts the first fatal throwable into a safe class-and-message string, broadcasts it before shutting RMI down, and the reader throws that reason immediately. Schema lookup stops swallowing missing-table errors by preparing metadata SQL for every non-empty ordinary table name.

**Tech Stack:** Scala 2.12.17, Spark DataSource V2 3.4.1, Java RMI, Maven, PostgreSQL JDBC, shaded JAR

---

### Task 1: Add executor abort state and RMI failure payload

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/TransferAbortState.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/PartitionControlBlock.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMIMaster.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/TransferAbortStateTest.scala`

- [ ] **Step 1: Write the failing abort-state test**

Create a main-style regression test that verifies the first reason wins, an abort throws immediately with the MDB text, and an unset reason has a deterministic fallback:

```scala
package com.itsumma.gpconnector.rmi

object TransferAbortStateTest {
  def main(args: Array[String]): Unit = {
    val state = new TransferAbortState("query-1", "0:1:0")
    state.abort("org.postgresql.util.PSQLException: ERROR: relation \"cdm_dwyz.not_exist_table_xxx\" does not exist")
    state.abort("cleanup failure")

    val thrown = expectFailure { state.throwIfAborted() }
    assert(thrown.getMessage.contains("relation \"cdm_dwyz.not_exist_table_xxx\" does not exist"))
    assert(!thrown.getMessage.contains("cleanup failure"))

    val fallback = new TransferAbortState("query-2", "2:3:0")
    fallback.aborted.set(true)
    assert(expectFailure { fallback.throwIfAborted() }.getMessage.contains("query-2"))
    println("TRANSFER_ABORT_STATE_TEST_OK")
  }

  private def expectFailure(body: => Unit): Exception = {
    try {
      body
      throw new AssertionError("Expected abort failure")
    } catch {
      case e: Exception => e
    }
  }
}
```

- [ ] **Step 2: Compile the test to verify it fails**

Run:

```bash
mvn -pl spark-greenplum-connector -am test-compile -DskipTests
```

Expected: compilation fails because `TransferAbortState` does not exist.

- [ ] **Step 3: Implement first-reason abort state**

Create `TransferAbortState.scala`:

```scala
package com.itsumma.gpconnector.rmi

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

private[gpconnector] final class TransferAbortState(queryId: String, instanceId: String) {
  val aborted: AtomicBoolean = new AtomicBoolean(false)
  private val reason = new AtomicReference[String](null)

  def abort(message: String): Unit = {
    Option(message).map(_.trim).filter(_.nonEmpty).foreach(reason.compareAndSet(null, _))
    aborted.set(true)
  }

  def throwIfAborted(): Unit = {
    if (aborted.get()) {
      val message = Option(reason.get()).getOrElse(
        s"Job $queryId aborted while reading executor instance $instanceId")
      throw new Exception(message)
    }
  }
}
```

- [ ] **Step 4: Extend the RMI control block without changing existing constructor callers**

Append `failureMessage: String = null` after `gpfdistUrl`, document it in Scaladoc, and include only a boolean `hasFailure` in `toString` so database details are not duplicated in routine control-block logs.

```scala
gpfdistUrl: String = null,
failureMessage: String = null
```

- [ ] **Step 5: Broadcast the failure reason from the master**

Change `broadcastCoordinatorCommand` to accept an optional message and send a copied control block:

```scala
private def broadcastCoordinatorCommand(cmd: String, failureMessage: String = null): Unit = {
  // snapshot existing instances under synchronization
  val commandPcb = instance._2.copy(failureMessage = failureMessage)
  instance._2.handler.coordinatorAsks(commandPcb, cmd)
}
```

In `failJob`, retain the first non-empty `abortMsg`, set failure flags, capture that retained value under synchronization, then call:

```scala
broadcastCoordinatorCommand("sqlTransferAbort", failureMessage)
```

In `retryBatch`, pass its current message explicitly so retry readers also receive the reason.

- [ ] **Step 6: Make the slave record and throw the RMI reason**

Replace the standalone `jobAbort` allocation with:

```scala
private val transferAbort = new TransferAbortState(queryId, instanceId)
val jobAbort: AtomicBoolean = transferAbort.aborted
```

Handle abort commands with:

```scala
transferAbort.abort(newPcb.failureMessage)
coordinatorSqlComplete.set(true)
```

In `read`, call `transferAbort.throwIfAborted()` immediately after the wait loop and before timeout/EOF handling.

- [ ] **Step 7: Compile and run the abort-state test**

Run `test-compile`, build the test classpath, then execute the test main class. Expected final line:

```text
TRANSFER_ABORT_STATE_TEST_OK
```

- [ ] **Step 8: Commit Task 1**

```bash
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/TransferAbortStateTest.scala
git commit -m "fix: propagate transfer abort reason to Spark readers"
```

### Task 2: Fail the driver SQL thread and preserve the MDB root error

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/TransferFailure.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala`
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/TransferFailureTest.scala`

- [ ] **Step 1: Write the failing root-message test**

```scala
package com.itsumma.gpconnector.rmi

import java.sql.SQLException

object TransferFailureTest {
  def main(args: Array[String]): Unit = {
    val db = new SQLException("ERROR: relation \"cdm_dwyz.not_exist_table_xxx\" does not exist")
    val wrapped = new RuntimeException("wrapper", db)
    val message = TransferFailure.message(wrapped)
    assert(message.contains("java.sql.SQLException"))
    assert(message.contains("relation \"cdm_dwyz.not_exist_table_xxx\" does not exist"))
    assert(!message.contains("wrapper"))
    println("TRANSFER_FAILURE_TEST_OK")
  }
}
```

- [ ] **Step 2: Compile the test to verify it fails**

Expected: compilation fails because `TransferFailure` does not exist.

- [ ] **Step 3: Implement root-cause message normalization**

Create a package-visible helper that walks the cause chain defensively and returns only canonical class name plus non-empty message:

```scala
package com.itsumma.gpconnector.rmi

private[gpconnector] object TransferFailure {
  def message(failure: Throwable): String = {
    var root = failure
    while (root.getCause != null && (root.getCause ne root)) root = root.getCause
    val className = Option(root.getClass.getCanonicalName).getOrElse(root.getClass.getName)
    Option(root.getMessage).map(_.trim).filter(_.nonEmpty)
      .map(message => s"$className: $message")
      .getOrElse(className)
  }
}
```

- [ ] **Step 4: Add the fatal catch around the whole SQL-thread loop**

In `GreenplumScan.SqlThread.run()` keep the existing loop in `try`, add a `catch` before the existing `finally`, and perform failure publication before cleanup:

```scala
} catch {
  case failure: Throwable =>
    processing.set(false)
    aborted.set(true)
    val message = TransferFailure.message(failure)
    logError(s"SqlThread queryId=$queryId aborted: $message", failure)
    if (rmiMaster != null) {
      try rmiMaster.failJob(message, notifySlaves = true)
      catch {
        case notifyFailure: Throwable =>
          logWarning(s"Unable to notify Spark readers about queryId=$queryId failure: " +
            s"${TransferFailure.message(notifyFailure)}")
      }
    }
} finally {
```

Do not rethrow from the daemon thread; Spark observes the same failure through reader task exceptions. Keep `rmiMaster.stop()` in `finally`, after synchronous `failJob()` broadcasting.

- [ ] **Step 5: Compile and run the failure-message test**

Expected final line:

```text
TRANSFER_FAILURE_TEST_OK
```

- [ ] **Step 6: Commit Task 2**

```bash
git add spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/TransferFailure.scala spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/TransferFailureTest.scala
git commit -m "fix: abort Spark read tasks on driver SQL failure"
```

### Task 3: Fail missing ordinary tables during schema binding

**Files:**
- Modify: `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`
- Create: `spark-greenplum-connector/src/test/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaLookupTest.scala`

- [ ] **Step 1: Write the failing schema-SQL selection test**

```scala
package org.apache.spark.sql.itsumma.gpconnector

object SparkSchemaLookupTest {
  def main(args: Array[String]): Unit = {
    assert(SparkSchemaUtil.schemaLookupSql("cdm_dwyz.orders").contains(
      "select * from cdm_dwyz.orders"))
    assert(SparkSchemaUtil.schemaLookupSql("select id from orders").contains(
      "select id from orders"))
    assert(SparkSchemaUtil.schemaLookupSql("   ").isEmpty)
    println("SPARK_SCHEMA_LOOKUP_TEST_OK")
  }
}
```

- [ ] **Step 2: Compile the test to verify it fails**

Expected: compilation fails because `schemaLookupSql` does not exist.

- [ ] **Step 3: Implement schema SQL selection and remove swallowed table errors**

Add:

```scala
private[gpconnector] def schemaLookupSql(tableOrQuery: String): Option[String] = {
  val target = tableOrQuery.trim
  if (target.isEmpty) None
  else if (GPTarget(target).isQuery) Some(target)
  else Some(s"select * from $target")
}
```

Replace the `GPClient.tableExists`/empty-`StructType` branch with:

```scala
schemaLookupSql(tableOrQuery) match {
  case None => return new StructType()
  case Some(sql) =>
    if (!conn.getAutoCommit) conn.commit()
    using(conn.prepareStatement(sql)) { stmt =>
      // existing metadata conversion remains unchanged
    }
}
```

This preserves the placeholder only for an empty table name used by custom `sqlTransfer`; an ordinary missing table reaches PostgreSQL metadata preparation and its `SQLException` propagates from `bind`.

- [ ] **Step 4: Compile and run the schema lookup and CN2 regression tests**

Expected final lines:

```text
SPARK_SCHEMA_LOOKUP_TEST_OK
SERVER_PUBLISH_MAPPING_TEST_OK
```

- [ ] **Step 5: Commit Task 3**

```bash
git add spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala spark-greenplum-connector/src/test/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaLookupTest.scala
git commit -m "fix: surface missing MDB tables during view binding"
```

### Task 4: Build, inspect, and deliver the complete connector JAR

**Files:**
- Verify: all modified Scala sources and regression tests
- Create artifact: `spark-ymatrix-connector_2.12-3.1_YYYYMMDD_HHMMSS.jar` in the original workspace root

- [ ] **Step 1: Run whitespace and source-diff checks**

```bash
git diff --check b95c9fb..HEAD
git status --short
```

Expected: no whitespace errors; only intended source/test/plan files are committed.

- [ ] **Step 2: Run all main-style automated regression tests**

Compile test sources once, generate the Maven test classpath, and run:

```text
TransferAbortStateTest
TransferFailureTest
SparkSchemaLookupTest
ServerPublishMappingTest
```

Expected: each prints its `_TEST_OK` marker and exits zero.

- [ ] **Step 3: Package the shaded JAR on `172.16.100.143`**

Sync only the committed plan/source/test files to `/root/spark-greenplum-connector`, then run the project Maven package command with tests skipped because the main-style tests already ran explicitly:

```bash
mvn clean package -DskipTests
```

Expected: Maven `BUILD SUCCESS` and a timestamped shaded connector JAR under `spark-greenplum-connector/target`.

- [ ] **Step 4: Inspect JAR integrity and contents**

Verify archive readability and the presence of:

```text
com/itsumma/gpconnector/reader/GreenplumScan$SqlThread.class
com/itsumma/gpconnector/rmi/RMIMaster.class
com/itsumma/gpconnector/rmi/RMISlave.class
com/itsumma/gpconnector/rmi/TransferAbortState.class
com/itsumma/gpconnector/rmi/TransferFailure$.class
org/postgresql/Driver.class
org/apache/commons/dbcp2/BasicDataSource.class
```

- [ ] **Step 5: Copy the JAR to the original workspace with a second-resolution timestamp**

Use the required name:

```text
spark-ymatrix-connector_2.12-3.1_YYYYMMDD_HHMMSS.jar
```

Compute SHA-256 and byte size after copy.

- [ ] **Step 6: Merge only the fix commits back to the original dirty worktree**

Cherry-pick the plan and three implementation commits onto `main`; verify the pre-existing `status.md`, `todo.md`, manuals, and earlier JARs remain untouched.

- [ ] **Step 7: Final handoff**

Report the local clickable JAR path, SHA-256, size, implementation commits, automated test markers, and the explicit boundary that no live MDB business validation was performed.
