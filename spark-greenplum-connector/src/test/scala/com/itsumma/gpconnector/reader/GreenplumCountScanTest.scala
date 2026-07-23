package com.itsumma.gpconnector.reader

import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.atomic.AtomicInteger

object GreenplumCountScanTest {
  def main(args: Array[String]): Unit = {
    assert(CountSql.build(
      "cdm_dwyz.orders",
      "amount > 50",
      "cdm_dwyz") ==
      "select count(*)::bigint as connector_count from " +
        "cdm_dwyz.orders where amount > 50")
    assert(CountSql.build(
      "select * from orders",
      "",
      "public") ==
      "select count(*)::bigint as connector_count from " +
        "(select * from orders) connector_count_source")

    val options = GPOptionsFactory(Map(
      "url" -> "jdbc:postgresql://localhost/test",
      "dbtable" -> "cdm_dwyz.orders"
    ))
    val scan = new GreenplumCountScan(
      options,
      "cdm_dwyz.orders",
      "amount > 50")
    assert(scan.toBatch.planInputPartitions().length == 1)
    assert(scan.readSchema() == CountPushdown.OutputSchema)

    val nextCalls = new AtomicInteger(0)
    val resultSet = proxy(classOf[ResultSet]) { (method, _) =>
      method.getName match {
        case "next" =>
          java.lang.Boolean.valueOf(nextCalls.getAndIncrement() == 0)
        case "getLong" => Long.box(123L)
        case "wasNull" => java.lang.Boolean.FALSE
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val statement = proxy(classOf[PreparedStatement]) { (method, _) =>
      method.getName match {
        case "executeQuery" => resultSet
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val connection = connectionReturning(statement)
    val reader = new GreenplumCountPartitionReader(
      null,
      "cdm_dwyz.orders",
      "amount > 50",
      connection,
      "cdm_dwyz")
    assert(reader.next())
    assert(reader.get().getLong(0) == 123L)
    assert(!reader.next())
    reader.close()

    val databaseFailure = new SQLException("MDB count failed")
    val failingStatement = proxy(classOf[PreparedStatement]) { (method, _) =>
      method.getName match {
        case "executeQuery" => throw databaseFailure
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val failingReader = new GreenplumCountPartitionReader(
      null,
      "cdm_dwyz.orders",
      "",
      connectionReturning(failingStatement),
      "cdm_dwyz")
    assert(expectSqlFailure(failingReader.next()).getMessage == "MDB count failed")
    println("GREENPLUM_COUNT_SCAN_TEST_OK")
  }

  private def connectionReturning(
      statement: PreparedStatement): Connection =
    proxy(classOf[Connection]) { (method, _) =>
      method.getName match {
        case "prepareStatement" => statement
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }

  private def expectSqlFailure(body: => Boolean): SQLException = {
    try {
      body
      throw new AssertionError("Expected SQLException")
    } catch {
      case failure: SQLException => failure
    }
  }

  private def proxy[T](
      api: Class[T])(
      handler: (Method, Array[AnyRef]) => AnyRef): T = {
    Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array[Class[_]](api),
      new InvocationHandler {
        override def invoke(
            proxy: Any,
            method: Method,
            args: Array[AnyRef]): AnyRef =
          handler(method, args)
      }).asInstanceOf[T]
  }

  private def defaultValue(returnType: Class[_]): AnyRef = {
    if (!returnType.isPrimitive) null
    else if (returnType == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
    else if (returnType == java.lang.Integer.TYPE) Int.box(0)
    else if (returnType == java.lang.Long.TYPE) Long.box(0L)
    else if (returnType == java.lang.Double.TYPE) Double.box(0D)
    else if (returnType == java.lang.Float.TYPE) Float.box(0F)
    else if (returnType == java.lang.Short.TYPE) Short.box(0.toShort)
    else if (returnType == java.lang.Byte.TYPE) Byte.box(0.toByte)
    else if (returnType == java.lang.Character.TYPE) Char.box(0.toChar)
    else null
  }
}
