package com.itsumma.gpconnector.gpfdist

import org.apache.spark.sql.itsumma.gpconnector.PortRange

import java.net.BindException

object PortBinder {
  def createInRange(portRange: PortRange, create: Int => WebServer): WebServer = {
    var lastFailure: Throwable = null
    for (port <- portRange.ports) {
      try {
        return create(port)
      } catch {
        case ex: BindException =>
          lastFailure = ex
      }
    }

    val detail = Option(lastFailure).map(ex => s": ${ex.getMessage}").getOrElse("")
    throw new BindException(s"No available gpfdist port in configured range ${portRange}${detail}")
  }
}
