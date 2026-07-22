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
