package com.itsumma.gpconnector.rmi

private[gpconnector] final case class ReadStartupState(
    hasRowsOrCanRead: Boolean,
    isNormalEof: Boolean,
    failureMessage: Option[String]) {

  def throwIfFailed(): Unit =
    failureMessage.foreach(message =>
      throw new IllegalStateException(message))
}

private[gpconnector] object ReadStartupState {
  val connected: ReadStartupState =
    ReadStartupState(
      hasRowsOrCanRead = true,
      isNormalEof = false,
      failureMessage = None)

  val noWork: ReadStartupState =
    ReadStartupState(
      hasRowsOrCanRead = false,
      isNormalEof = true,
      failureMessage = None)

  val disconnected: ReadStartupState =
    failed("read coordinator is not connected")

  def failed(message: String): ReadStartupState =
    ReadStartupState(
      hasRowsOrCanRead = false,
      isNormalEof = false,
      failureMessage = Some(message))
}
