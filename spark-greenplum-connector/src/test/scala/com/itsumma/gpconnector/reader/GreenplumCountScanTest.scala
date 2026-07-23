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

    val validResultSet = countResultSet(
      hasRow = true,
      countValue = 123L,
      nullValue = false)
    val validStatement = statementReturning(validResultSet)
    val validConnection = connectionReturning(validStatement)
    assert(DriverCountQuery.execute(
      validConnection,
      "cdm_dwyz.orders",
      "amount > 50",
      "cdm_dwyz") == 123L)

    val options = GPOptionsFactory(Map(
      "url" -> "jdbc:postgresql://localhost/test",
      "dbtable" -> "cdm_dwyz.orders"
    ))
    val loads = new AtomicInteger(0)
    val scan = new GreenplumCountScan(
      options,
      null,
      "cdm_dwyz.orders",
      "amount > 50",
      () => {
        loads.incrementAndGet()
        123L
      })
    assert(loads.get() == 0)
    val firstPlan = scan.toBatch.planInputPartitions()
    val secondPlan = scan.toBatch.planInputPartitions()
    assert(loads.get() == 1)
    assert(firstPlan.length == 1)
    assert(firstPlan.head.asInstanceOf[GreenplumCountInputPartition]
      .countValue == 123L)
    assert(secondPlan.head.asInstanceOf[GreenplumCountInputPartition]
      .countValue == 123L)
    assert(scan.readSchema() == CountPushdown.OutputSchema)

    val reader =
      GreenplumCountReaderFactory.createReader(firstPlan.head)
    assert(reader.next())
    assert(reader.get().getLong(0) == 123L)
    assert(!reader.next())
    assert(expectFailure[IllegalStateException](reader.get())
      .getMessage == "get() called without a current COUNT row")
    reader.close()

    val databaseFailure = new SQLException("MDB count failed")
    val failingStatement = proxy(classOf[PreparedStatement]) { (method, _) =>
      method.getName match {
        case "executeQuery" => throw databaseFailure
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val propagated = expectFailure[SQLException] {
      DriverCountQuery.execute(
        connectionReturning(failingStatement),
        "cdm_dwyz.orders",
        "",
        "cdm_dwyz")
    }
    assert(propagated eq databaseFailure)

    val emptyFailure = expectFailure[IllegalStateException] {
      DriverCountQuery.execute(
        connectionReturning(statementReturning(countResultSet(
          hasRow = false,
          countValue = 0L,
          nullValue = false))),
        "cdm_dwyz.orders",
        "",
        "cdm_dwyz")
    }
    assert(emptyFailure.getMessage.startsWith(
      "COUNT query returned no row:"))

    val nullFailure = expectFailure[IllegalStateException] {
      DriverCountQuery.execute(
        connectionReturning(statementReturning(countResultSet(
          hasRow = true,
          countValue = 0L,
          nullValue = true))),
        "cdm_dwyz.orders",
        "",
        "cdm_dwyz")
    }
    assert(nullFailure.getMessage.startsWith(
      "COUNT query returned NULL:"))

    val extraRows = new AtomicInteger(0)
    val multiRowResultSet = proxy(classOf[ResultSet]) { (method, _) =>
      method.getName match {
        case "next" =>
          java.lang.Boolean.valueOf(
            extraRows.getAndIncrement() < 2)
        case "getLong" => Long.box(123L)
        case "wasNull" => java.lang.Boolean.FALSE
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val multiRowFailure = expectFailure[IllegalStateException] {
      DriverCountQuery.execute(
        connectionReturning(statementReturning(multiRowResultSet)),
        "cdm_dwyz.orders",
        "",
        "cdm_dwyz")
    }
    assert(multiRowFailure.getMessage.startsWith(
      "COUNT query returned more than one row:"))

    val queryFailure = new SQLException("query failure")
    val closeFailure = new SQLException("statement close failure")
    val cleanupStatement = proxy(classOf[PreparedStatement]) { (method, _) =>
      method.getName match {
        case "executeQuery" => throw queryFailure
        case "close" => throw closeFailure
        case _ => defaultValue(method.getReturnType)
      }
    }
    val failureWithSuppressed = expectFailure[SQLException] {
      DriverCountQuery.execute(
        connectionReturning(cleanupStatement),
        "cdm_dwyz.orders",
        "",
        "cdm_dwyz")
    }
    assert(failureWithSuppressed eq queryFailure)
    assert(failureWithSuppressed.getSuppressed.sameElements(
      Array[Throwable](closeFailure)))

    println("GREENPLUM_COUNT_SCAN_TEST_OK")
  }

  private def countResultSet(
      hasRow: Boolean,
      countValue: Long,
      nullValue: Boolean): ResultSet = {
    val nextCalls = new AtomicInteger(0)
    proxy(classOf[ResultSet]) { (method, _) =>
      method.getName match {
        case "next" =>
          java.lang.Boolean.valueOf(
            hasRow && nextCalls.getAndIncrement() == 0)
        case "getLong" => Long.box(countValue)
        case "wasNull" => java.lang.Boolean.valueOf(nullValue)
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
  }

  private def statementReturning(
      resultSet: ResultSet): PreparedStatement =
    proxy(classOf[PreparedStatement]) { (method, _) =>
      method.getName match {
        case "executeQuery" => resultSet
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
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

  private def expectFailure[T <: Throwable](
      body: => Any)(
      implicit expectedClass: Manifest[T]): T = {
    try {
      body
      throw new AssertionError(
        s"Expected ${expectedClass.runtimeClass.getName}")
    } catch {
      case failure
          if expectedClass.runtimeClass.isInstance(failure) =>
        failure.asInstanceOf[T]
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
