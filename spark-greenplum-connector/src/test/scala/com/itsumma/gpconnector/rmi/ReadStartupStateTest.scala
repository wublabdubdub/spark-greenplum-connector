package com.itsumma.gpconnector.rmi

object ReadStartupStateTest {
  def main(args: Array[String]): Unit = {
    assert(ReadStartupState.connected.hasRowsOrCanRead)
    assert(!ReadStartupState.connected.isNormalEof)
    assert(ReadStartupState.noWork.isNormalEof)
    assert(!ReadStartupState.noWork.hasRowsOrCanRead)

    assert(expectFailure {
      ReadStartupState.failed("original MDB error").throwIfFailed()
    }.getMessage.contains("original MDB error"))
    assert(expectFailure {
      ReadStartupState.disconnected.throwIfFailed()
    }.getMessage.contains("not connected"))
    println("READ_STARTUP_STATE_TEST_OK")
  }

  private def expectFailure(body: => Unit): Exception = {
    try {
      body
      throw new AssertionError("Expected read startup failure")
    } catch {
      case failure: Exception => failure
    }
  }
}
