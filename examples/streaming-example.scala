// 以下几行是历史导入示例，当前脚本未直接使用，先保留做参考。
//import org.apache.spark.sql.SaveMode
//import java.util.UUID.randomUUID
//import java.time._
//import org.apache.hadoop.conf.Configuration
//import org.apache.spark.sql.{SparkSession}
//import org.apache.spark.sql.streaming.Trigger
//import org.apache.spark.{SparkContext, TaskContext}

// 导入 SparkContext，用于清理 checkpoint 目录。
import org.apache.spark.SparkContext
// 导入 Hadoop 文件系统类型，用于删除目录。
import org.apache.hadoop.fs.{FileSystem, Path}
// 导入 Spark SQL 常用函数。
import org.apache.spark.sql.functions._
// 导入 URI，用于定位 checkpoint 所在文件系统。
import java.net.URI

/**
 * 这个示例建议在 spark-shell 中运行。
 * 默认假设你从当前脚本和 log4j.properties 所在目录启动 spark-shell。
 */

// 参考启动命令：
//spark-shell --files "./log4j.properties" --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.ui.showConsoleProgress=false"

/**
 * 在 spark-shell 提示符中执行：
 * scala> :load streaming-example.scala
 */

/**
 * 首先请把这里替换成你自己的 Greenplum / YMatrix 连接参数。
 */
// 定义数据库 JDBC 地址。
val dbUrl = "jdbc:postgresql://ymatrix-master-host:5432/db-name"
// 定义数据库用户名。
val dbUser = "your_ymatrix_db_user"
// 定义数据库密码。
val dbPassword = "ymatrix_user_password"

/**
 * 这个示例会启动两个 connector 实例，并通过 Structured Streaming 的 micro-batch 模式串起来。
 *
 * 第一段流：
 * 通过 YMatrix 侧 PL/pgSQL 脚本持续生成模拟数据。
 *
 * 第二段流：
 * 接收这些数据，再把结果写回 YMatrix 侧进行统计输出。
 *
 * cpDirName:
 * Spark Structured Streaming 的 checkpoint 路径。
 *
 * secondsPerBatch:
 * 期望的微批间隔秒数。
 *
 * rowsPerBatch:
 * 每个批次生成的行数。
 *
 * payloadSize:
 * 每条记录里的大文本 payload 大小，用于模拟较宽数据。
 */
// 定义 checkpoint 目录。
val cpDirName = "/tmp/db2db-stream/checkpoint"
// 定义微批的目标时长；0.0 表示尽量快。
val secondsPerBatch: Double = 0.0
// 定义每批生成的记录数。
val rowsPerBatch: Int = 2000
// 定义 payload 文本大小。
val payloadSize: Int = 100000
// val rowsPerBatch: Int = 200000
// val payloadSize: Int = 10

// 根据目标微批时长，推导 offset 时间缩放因子。
val offsetScale: Double = if (secondsPerBatch > 0.1) 1.0 / secondsPerBatch else 10.0

// 删除文件系统上的指定路径，通常用于清理旧 checkpoint。
def cleanFS(sc: SparkContext, fsPath: String) = {
  // 根据给定 URI 和 Spark 的 Hadoop 配置定位文件系统。
  val fs = org.apache.hadoop.fs.FileSystem.get(new URI(fsPath), sc.hadoopConfiguration)
  // 递归删除目标路径。
  fs.delete(new Path(fsPath), true)
}

/**
 * 每次运行前都先清理 checkpoint。
 * 如果你希望应用重启后从上次已完成 offset 继续跑，可以把下面这行注释掉。
 */
cleanFS(sc, cpDirName)

/**
 * 这个 PL/pgSQL 脚本充当“发送端生成器”。
 * connector 会把尖括号模板变量替换成实际值。
 */
// 定义发送端生成脚本。
val generator = s"""do $$$$
declare
  v_start_offset bigint := ('<start_offset_json>'::json ->> 'offset_ts')::bigint;
  v_end_ofsset bigint := ('<end_offset_json>'::json ->> 'offset_ts')::bigint;
  v_batch_size bigint := v_end_ofsset - v_start_offset;
  v_sleep float := 0.0;
  v_counter bigint := ${rowsPerBatch};
  v_rec_per_offset bigint := 0;
  v_id bigint := 0;
  v_dur float := ${secondsPerBatch};
begin
  if v_batch_size = 0 then
    return;
  end if;
  v_rec_per_offset := v_counter / v_batch_size;
  if v_rec_per_offset = 0 then
    v_rec_per_offset := 1;
  end if;
  v_id := v_start_offset * v_rec_per_offset;
  if v_counter > 0 and v_counter <= 100 and v_dur > 0.0 then
    v_sleep := v_dur / v_counter::float;
  end if;
  insert into <ext_table>
  select  <select_colList>
  from  (
        select  seq_n::bigint + v_id id,
                seq_n::bigint,
                clock_timestamp() gen_ts,
                (seq_n::bigint + v_id) / v_rec_per_offset + 1 offset_id,
                repeat('0', ${payloadSize})::text payload,
                case when v_sleep >= 0.01 then pg_sleep(v_sleep) else null end sleep
        from    generate_series(1, v_counter) as seq_n(n)
        ) a;
  v_sleep := v_dur - extract(epoch from clock_timestamp()-now())::float;
  if v_sleep >= 0.01 then
    perform pg_sleep(v_sleep);
  end if;
  raise notice '% records generated for offsets % - %', v_counter, v_start_offset + 1, v_end_ofsset + 1;
end
$$$$"""

/**
 * 这个 PL/pgSQL 脚本充当“接收端聚合器”。
 * 它会把当前微批收到的数据写入一张目标表，并输出统计信息。
 */
// 定义接收端聚合脚本。
val aggregator = """do $$
declare
  v_cnt int;
begin
  drop table if exists db2db_target;
  create table db2db_target
  with ( appendoptimized=true, blocksize=2097152 )
  as select  *
  from    <ext_table> us
  DISTRIBUTED BY (id)
  ;
  GET DIAGNOSTICS v_cnt := ROW_COUNT;
  raise notice 'Read % rows in %s', v_cnt, extract(epoch from clock_timestamp()-now());
end
$$"""

/**
 * connector 还提供了一些额外 UDF。
 * 这里先把它们注册到 Spark SQL 里。
 */
// 注册 connector 附带的 UDF。
com.itsumma.gpconnector.ItsMiscUDFs.registerUDFs()
// 打印 connector 版本，便于排障。
println(s"Connector version: ${com.itsumma.gpconnector.ItsMiscUDFs.getVersion}")

// 构造“读取生成流 -> 增加统计列 -> 写回数据库”的完整流式任务。
var stream = (spark.readStream.format("its-ymatrix").option("url", dbUrl).
  // 指定用户名。
  option("user", dbUser).
  // 指定密码。
  option("password", dbPassword).
  /**
  * dbtable 在这里用于声明输出列的结构，必须与 generator 产出的列匹配。
  */
  option("dbtable","select 1::bigint id, 1::int seq_n, clock_timestamp() gen_ts, 1::bigint offset_id, '0'::text payload").
  // 指定发送端生成脚本。
  option("sqlTransfer", generator).
  // 指定 offset 查询逻辑，用当前时间驱动微批推进。
  option("offset.select", s"select json_build_object('offset_ts', (extract(epoch from pg_catalog.clock_timestamp()) * ${offsetScale})::bigint)::text").
  // 指定数据库消息级别。
  option("dbmessages", "WARN").
  //option("buffer.size", "20000000").
  // 启动流式读取。
  load().
  /**
   * getRowTimestamp() 返回一条记录进入 Spark 的时间戳。
  */
  withColumn("spark_ts", com.itsumma.gpconnector.ItsMiscUDFs.getRowTimestamp()).
  /**
   * getBatchId() 返回当前微批编号。
  */
  selectExpr("getBatchId() as batch_id", "id", "seq_n", "gen_ts", "spark_ts", "(cast(spark_ts as double) - cast(gen_ts as double)) as delay_s", "offset_id", "payload").
  //repartition(4).
writeStream.
  // 使用 its-ymatrix connector 作为 sink。
  format("its-ymatrix").option("url", dbUrl).
  // 指定用户名。
  option("user", dbUser).
  // 指定密码。
  option("password", dbPassword).
  // 指定接收端聚合脚本。
  option("sqlTransfer", aggregator).
  // 指定数据库消息级别。
  option("dbmessages", "WARN").
  // 指定 checkpoint 位置。
  option("checkpointLocation", cpDirName).
  // 打开异步进度跟踪。
  option("asyncProgressTrackingEnabled", true).
  // 设置异步进度 checkpoint 间隔。
  option("asyncProgressTrackingCheckpointIntervalMs", 120000).
  // 给当前动作命名，便于日志观察。
  option("action.name", "console").
  //option("buffer.size", "20000000").
  // 使用 append 输出模式。
  outputMode("append").
  // 启动流任务。
  start())

/**
 * 在生产代码里，你通常会调用 awaitTermination() 来阻塞等待流任务结束。
 */
//val ret = stream.awaitTermination()

/**
 * 在 spark-shell 中不一定需要 awaitTermination()。
 * 如果想手动结束，可以直接调用 stream.stop()。
 */
//stream.stop()
