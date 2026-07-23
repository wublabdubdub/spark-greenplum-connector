package com.itsumma.gpconnector.rmi

import java.rmi.Remote
import java.rmi.server.UnicastRemoteObject

object RmiObjectLifecycleTest {
  private trait Ping extends Remote
  private final class PingImpl
    extends UnicastRemoteObject
      with Ping

  def main(args: Array[String]): Unit = {
    val remote = new PingImpl
    assert(RmiObjectLifecycle.forceUnexport(remote))
    assert(!RmiObjectLifecycle.forceUnexport(remote))
    println("RMI_OBJECT_LIFECYCLE_TEST_OK")
  }
}
