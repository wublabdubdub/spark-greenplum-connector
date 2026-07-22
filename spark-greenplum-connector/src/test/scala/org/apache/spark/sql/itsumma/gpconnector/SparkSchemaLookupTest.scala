package org.apache.spark.sql.itsumma.gpconnector

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, PreparedStatement, SQLException}
import java.util.concurrent.atomic.AtomicInteger

object SparkSchemaLookupTest {
  def main(args: Array[String]): Unit = {
    assert(SparkSchemaUtil.schemaLookupSql("cdm_dwyz.orders").contains(
      "select * from cdm_dwyz.orders"))
    assert(SparkSchemaUtil.schemaLookupSql("select id from orders").contains(
      "select id from orders"))
    assert(SparkSchemaUtil.schemaLookupSql("   ").isEmpty)

    val prepareCount = new AtomicInteger(0)
    val rollbackCount = new AtomicInteger(0)
    val missingTable = new SQLException(
      "ERROR: relation \"cdm_dwyz.not_exist_table_xxx\" does not exist")
    val statement = proxy(classOf[PreparedStatement]) { (method, _) =>
      method.getName match {
        case "executeQuery" | "getMetaData" => throw missingTable
        case "close" => null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val connection = proxy(classOf[Connection]) { (method, _) =>
      method.getName match {
        case "getAutoCommit" => java.lang.Boolean.FALSE
        case "prepareStatement" =>
          prepareCount.incrementAndGet()
          statement
        case "commit" | "close" => null
        case "rollback" =>
          rollbackCount.incrementAndGet()
          null
        case _ => defaultValue(method.getReturnType)
      }
    }
    val options = GPOptionsFactory(Map(
      "url" -> "jdbc:postgresql://localhost/test",
      "dbtable" -> "cdm_dwyz.not_exist_table_xxx",
      "applicationname" -> "spark-schema-lookup-test"
    ))
    val thrown = expectSqlFailure {
      SparkSchemaUtil.getGreenplumTableSchema(
        options, connection, "cdm_dwyz.not_exist_table_xxx")
    }
    assert(thrown.getMessage.contains("relation \"cdm_dwyz.not_exist_table_xxx\" does not exist"))
    assert(prepareCount.get() >= 2)
    assert(rollbackCount.get() >= 1)
    println("SPARK_SCHEMA_LOOKUP_TEST_OK")
  }

  private def proxy[T](api: Class[T])(handler: (Method, Array[AnyRef]) => AnyRef): T = {
    Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array[Class[_]](api),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          handler(method, args)
      }
    ).asInstanceOf[T]
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

  private def expectSqlFailure(body: => Unit): SQLException = {
    try {
      body
      throw new AssertionError("Expected missing-table SQLException")
    } catch {
      case e: SQLException => e
    }
  }
}
