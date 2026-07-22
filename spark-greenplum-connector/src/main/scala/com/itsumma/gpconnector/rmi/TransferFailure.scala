package com.itsumma.gpconnector.rmi

private[gpconnector] object TransferFailure {
  def message(failure: Throwable): String = {
    var root = failure
    while (root.getCause != null && (root.getCause ne root)) {
      root = root.getCause
    }
    val className = Option(root.getClass.getCanonicalName).getOrElse(root.getClass.getName)
    Option(root.getMessage).map(_.trim).filter(_.nonEmpty)
      .map(message => s"$className: $message")
      .getOrElse(className)
  }
}
