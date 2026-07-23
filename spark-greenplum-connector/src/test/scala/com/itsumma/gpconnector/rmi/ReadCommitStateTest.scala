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
