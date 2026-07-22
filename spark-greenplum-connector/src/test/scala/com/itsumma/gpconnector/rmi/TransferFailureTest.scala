package com.itsumma.gpconnector.rmi

import java.sql.SQLException

object TransferFailureTest {
  def main(args: Array[String]): Unit = {
    val db = new SQLException("ERROR: relation \"cdm_dwyz.not_exist_table_xxx\" does not exist")
    val wrapped = new RuntimeException("wrapper", db)
    val message = TransferFailure.message(wrapped)
    assert(message.contains("java.sql.SQLException"))
    assert(message.contains("relation \"cdm_dwyz.not_exist_table_xxx\" does not exist"))
    assert(!message.contains("wrapper"))
    println("TRANSFER_FAILURE_TEST_OK")
  }
}
