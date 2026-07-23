package com.itsumma.gpconnector.rmi

import java.rmi.{NoSuchObjectException, Remote}
import java.rmi.server.UnicastRemoteObject

private[gpconnector] object RmiObjectLifecycle {
  def forceUnexport(remote: Remote): Boolean =
    try {
      UnicastRemoteObject.unexportObject(remote, true)
    } catch {
      case _: NoSuchObjectException => false
    }
}
