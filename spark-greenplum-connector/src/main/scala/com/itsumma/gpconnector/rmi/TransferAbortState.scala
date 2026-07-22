package com.itsumma.gpconnector.rmi

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

private[gpconnector] final class TransferAbortState(queryId: String, instanceId: String) {
  val aborted: AtomicBoolean = new AtomicBoolean(false)
  private val reason = new AtomicReference[String](null)

  def abort(message: String): Unit = {
    Option(message).map(_.trim).filter(_.nonEmpty).foreach { value =>
      reason.compareAndSet(null, value)
    }
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
