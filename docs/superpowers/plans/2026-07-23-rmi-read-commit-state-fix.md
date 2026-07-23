# RMI Read Commit State Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent normally completed gpfdist reads from failing during `RMISlave.commit()` while preserving fail-fast behavior for real aborts and incomplete transfers.

**Architecture:** Add a package-private pure decision object that classifies the three existing completion flags without owning lifecycle state. Make both `RMISlave.commit()` validation points call that object, so normal and failure paths cannot diverge. Verify the decision table first, then run the connector's main-style regression tests and build the shaded JAR on `172.16.100.143`.

**Tech Stack:** Scala 2.12.17, Java 8, Spark 3.4.1, Maven, Paramiko/SFTP, Git

---

### Task 1: Add the failing completion-state regression test

**Files:**
- Create: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/ReadCommitStateTest.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.itsumma.gpconnector.rmi

object ReadCommitStateTest {
  def main(args: Array[String]): Unit = {
    assert(!ReadCommitState.shouldFail(
      jobAborted = false,
      coordinatorSqlComplete = true,
      sqlTransferComplete = true))

    assert(ReadCommitState.shouldFail(
      jobAborted = true,
      coordinatorSqlComplete = false,
      sqlTransferComplete = false))

    assert(ReadCommitState.shouldFail(
      jobAborted = false,
      coordinatorSqlComplete = true,
      sqlTransferComplete = false))

    assert(!ReadCommitState.shouldFail(
      jobAborted = false,
      coordinatorSqlComplete = false,
      sqlTransferComplete = false))

    println("READ_COMMIT_STATE_TEST_OK")
  }
}
```

- [ ] **Step 2: Upload the test-only source snapshot to 143**

Create a tar archive from the isolated worktree, excluding `.git` and existing
`target` directories so the uncommitted failing test is included. Upload it with Paramiko to
`root@172.16.100.143:/root/spark-ymatrix-rmi-fix-20260723/source.tar`, and extract it
under `/root/spark-ymatrix-rmi-fix-20260723/source`.

Expected: the remote directory contains `pom.xml` and
`spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/ReadCommitStateTest.scala`.

- [ ] **Step 3: Run test compilation to verify it fails**

Run on 143:

```bash
cd /root/spark-ymatrix-rmi-fix-20260723/source
mvn -pl spark-greenplum-connector -am test-compile -DskipTests
```

Expected: compilation fails because `ReadCommitState` does not exist.

- [ ] **Step 4: Commit the failing test**

```bash
git add spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/ReadCommitStateTest.scala
git commit -m "test: cover RMI read commit completion states"
```

### Task 2: Implement one authoritative commit-state decision

**Files:**
- Create: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/ReadCommitState.scala`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala:443-456`
- Modify: `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala:478-484`
- Test: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi/ReadCommitStateTest.scala`

- [ ] **Step 1: Add the pure state decision**

```scala
package com.itsumma.gpconnector.rmi

private[gpconnector] object ReadCommitState {
  def shouldFail(
      jobAborted: Boolean,
      coordinatorSqlComplete: Boolean,
      sqlTransferComplete: Boolean): Boolean =
    jobAborted || (coordinatorSqlComplete && !sqlTransferComplete)
}
```

- [ ] **Step 2: Replace the first duplicated predicate**

Replace:

```scala
if (jobAbort.get() || coordinatorSqlComplete.get()) {
```

with:

```scala
if (ReadCommitState.shouldFail(
    jobAbort.get(),
    coordinatorSqlComplete.get(),
    sqlTransferComplete.get())) {
```

Keep the existing buffered-transfer error message unchanged.

- [ ] **Step 3: Replace the final duplicated predicate**

Replace:

```scala
if (jobAbort.get() || (coordinatorSqlComplete.get() && !sqlTransferComplete.get()))
```

with:

```scala
if (ReadCommitState.shouldFail(
    jobAbort.get(),
    coordinatorSqlComplete.get(),
    sqlTransferComplete.get()))
```

Keep the existing final-transfer error message unchanged.

- [ ] **Step 4: Refresh the remote source snapshot**

Recreate the source tar archive from the isolated worktree, upload it to
`/root/spark-ymatrix-rmi-fix-20260723/source.tar`, replace only the temporary remote
build directory `/root/spark-ymatrix-rmi-fix-20260723/source`, and extract the new
snapshot.

Expected: the remote snapshot contains `ReadCommitState.scala` and both
`RMISlave.commit()` checks call `ReadCommitState.shouldFail`.

- [ ] **Step 5: Compile and run the focused test**

Run on 143:

```bash
cd /root/spark-ymatrix-rmi-fix-20260723/source
mvn -pl spark-greenplum-connector -am test-compile -DskipTests
mvn -q -pl spark-greenplum-connector dependency:build-classpath \
  -Dmdep.outputFile=/tmp/rmi-fix-classpath.txt
CP="spark-greenplum-connector/target/test-classes:spark-greenplum-connector/target/classes:$(cat /tmp/rmi-fix-classpath.txt)"
java -cp "$CP" com.itsumma.gpconnector.rmi.ReadCommitStateTest
```

Expected: compilation succeeds and output contains `READ_COMMIT_STATE_TEST_OK`.

- [ ] **Step 6: Commit the implementation**

```bash
git add \
  spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/ReadCommitState.scala \
  spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala
git commit -m "fix: distinguish completed RMI reads from aborts"
```

### Task 3: Run the connector regression suite on 143

**Files:**
- Verify: `spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/**/*.scala`

- [ ] **Step 1: Compile all test objects**

Run on 143:

```bash
cd /root/spark-ymatrix-rmi-fix-20260723/source
mvn -pl spark-greenplum-connector -am test-compile -DskipTests
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 2: Run all main-style connector regression tests**

Run on 143 with the `CP` created in Task 2:

```bash
for test_class in \
  com.itsumma.gpconnector.rmi.ReadCommitStateTest \
  com.itsumma.gpconnector.rmi.ReadStartupStateTest \
  com.itsumma.gpconnector.rmi.TransferAbortStateTest \
  com.itsumma.gpconnector.rmi.TransferFailureTest \
  com.itsumma.gpconnector.rmi.RmiObjectLifecycleTest \
  com.itsumma.gpconnector.reader.CountPushdownTest \
  com.itsumma.gpconnector.reader.GreenplumScanBuilderTest \
  com.itsumma.gpconnector.reader.ReadSchemaPlanTest \
  com.itsumma.gpconnector.reader.GreenplumCountScanTest \
  org.apache.spark.sql.itsumma.gpconnector.FilterPushdownTest \
  org.apache.spark.sql.itsumma.gpconnector.SparkSchemaLookupTest \
  org.apache.spark.sql.itsumma.gpconnector.ServerPublishMappingTest
do
  java -cp "$CP" "$test_class" || exit 1
done
```

Expected: all commands exit zero and each test prints its `_TEST_OK` marker.

- [ ] **Step 3: Inspect the exact implementation diff**

```bash
git diff 7523bd6..HEAD --check
git diff 7523bd6..HEAD -- \
  spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi \
  spark-greenplum-connector/src/test/scala/com/itsumma/gpconnector/rmi
```

Expected: no whitespace errors; only the decision object, its regression test, and
the two `RMISlave.commit()` predicates change.

### Task 4: Build and return the new shaded JAR

**Files:**
- Build: `pom.xml`
- Build: `spark-greenplum-connector/pom.xml`
- Produce remotely: `spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1_20260723.jar`
- Produce locally: `spark-ymatrix-connector_2.12-3.1_20260723_HHMMSS.jar`

- [ ] **Step 1: Run the clean Maven package build on 143**

```bash
cd /root/spark-ymatrix-rmi-fix-20260723/source
mvn clean package -DskipTests
```

Expected: `BUILD SUCCESS` and the shaded connector JAR exists in
`spark-greenplum-connector/target`.

- [ ] **Step 2: Record remote artifact metadata**

```bash
cd /root/spark-ymatrix-rmi-fix-20260723/source
ls -l spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1_20260723.jar
sha256sum spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1_20260723.jar
```

Expected: a non-empty JAR and one SHA-256 digest.

- [ ] **Step 3: Download without overwriting the earlier JAR**

Use Paramiko/SFTP to download the remote shaded JAR into the main local workspace.
Resolve the final filename at runtime with:

```powershell
$finalName = 'spark-ymatrix-connector_2.12-3.1_20260723_{0}.jar' -f `
  (Get-Date -Format 'HHmmss')
```

Expected: both the earlier `spark-ymatrix-connector_2.12-3.1_20260723.jar` and the
new timestamp-suffixed JAR remain present.

- [ ] **Step 4: Verify local integrity**

```powershell
$downloadedJar = Get-ChildItem -LiteralPath 'C:\四维纵横\工作材料\20260319 iceberg同步到YM' `
  -Filter 'spark-ymatrix-connector_2.12-3.1_20260723_*.jar' |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
$downloadedJar |
  Format-List FullName,Length,CreationTime,LastWriteTime
Get-FileHash -Algorithm SHA256 -LiteralPath $downloadedJar.FullName
```

Expected: local size and SHA-256 exactly match the remote artifact.

- [ ] **Step 5: Merge the isolated implementation commits into main**

From the main worktree:

```bash
git merge --ff-only fix/rmi-read-commit-state
```

Expected: main advances without touching the pre-existing uncommitted user files.
