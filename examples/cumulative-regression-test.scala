// 导入 SaveMode，用于 append / overwrite 写入模式。
import org.apache.spark.sql.SaveMode
// 导入 UUID 生成函数，用于构造随机主键。
import java.util.UUID.randomUUID
// 导入时间相关类型，用于生成时间戳字段。
import java.time._
// 导入隐式转换，供 RDD -> DataFrame 和 .as[T] 使用。
import spark.implicits._

/*
这个回归脚本会创建并写入 4 张测试表。

运行后可以在数据库侧执行下面的 SQL 查看结果：
select  '1: ' || count(1)::text uuid_seq from uuid_seq
union all
select  '2: ' || count(1)::text uuid_seq from uuid_seq_2
union all
select  '3: ' || count(1)::text uuid_seq from uuid_seq_3
union all
select  '4: ' || count(1)::text uuid_seq from uuid_seq_4
order by 1
*/

// 定义一个循环读的测试器，用于多次读取 uuid_seq 表。
case class LoopRead(opts: Map[String, String]) {
    // 执行循环读取逻辑。
    def run(): Unit = {
        // 初始化循环计数器。
        var cnt = 0
        // 连续执行 5 轮读取。
        while (cnt < 5) {
            // 轮次加 1。
            cnt += 1
            // 每一轮都读取更多的记录。
            val cmd = s"SELECT * FROM uuid_seq limit ${cnt + 5}"
            // 通过 connector 从 YMatrix 读取查询结果。
            val ymatrixDf = spark.read.format("its-ymatrix").options(opts.updated("dbtable", cmd)).load()
            // 打印分隔线。
            println("\r\n*************************************************")
            // 打印本轮预期说明。
            println(s"Should display ${cnt + 5} rows from uuid_seq table")
            // 打印分隔线。
            println("*************************************************")
            // 展示本轮读取结果。
            ymatrixDf.show(false)
        }
    }
}

// 定义一个循环写的测试器，用于多次向 uuid_seq_3 追加数据。
case class LoopWrite(opts: Map[String, String]) {
    // 执行循环写入逻辑。
    def run(): Unit = {
        // 初始化循环计数器。
        var cnt = 0
        // 连续执行 5 轮追加写。
        while (cnt < 5) {
            // 轮次加 1。
            cnt += 1
            // 打印分隔线。
            println("\r\n*************************************************")
            // 打印本轮预期说明。
            println(s"Should append 1000 rows into uuid_seq_3 table (pass ${cnt})")
            // 打印分隔线。
            println("*************************************************")
            // 在 SparkContext 上构造 1000 条随机测试数据并写入 uuid_seq_3。
            sc.
                // 生成 1000 个 UUID，并配上索引与时间戳。
                parallelize(Array.fill[String](1000){randomUUID().toString}.zipWithIndex.map({case (uid,id) => {(uid, id, java.sql.Timestamp.from(OffsetDateTime.now().toInstant), id % 2 == 1)}}), 2).
                // 转成 DataFrame 并指定列名。
                toDF("id", "seq_no", "created_d", "mv").
            // 开始写入。
            write.
                // 使用 its-ymatrix connector。
                format("its-ymatrix").
                // 把目标表改成 uuid_seq_3。
                options(opts.updated("dbtable","uuid_seq_3")).
                // 使用 append 模式。
                mode(SaveMode.Append).
                // 触发写入。
                save()

        }
    }
}

// 如有需要，可以打开这行调整日志级别。
//sc.setLogLevel("INFO")
// 定义连接参数与默认表名。
val options = Map("url"->"jdbc:postgresql://ymatrix-master-host:5432/db-name", "user"->"your_ymatrix_db_user", "password"->"ymatrix_user_password", "dbtable"->"uuid_seq")

// 构造第一批要写入 uuid_seq 的 1000 行测试数据。
var df = sc.
    // 生成随机 UUID、顺序号、创建时间和布尔字段。
    parallelize(Array.fill[String](1000){randomUUID().toString}.zipWithIndex.map({case (uid,id) => {(uid, id, java.sql.Timestamp.from(OffsetDateTime.now().toInstant), id % 2 == 1)}}), 2).
    // 转成 DataFrame。
    toDF("id", "seq_no", "created_d", "mv")
// 打印分隔线。
println("\r\n*************************************************")
// 打印预期说明。
println("Should append 1000 rows into uuid_seq table")
// 打印分隔线。
println("*************************************************")
// 把第一批数据追加写入 uuid_seq。
df.write.
    // 指定 connector。
    format("its-ymatrix").
    // 使用默认连接参数。
    options(options).
    // 采用 append 模式。
    mode(SaveMode.Append).
    // 触发写入。
    save()

// 从 uuid_seq 读回数据，验证读取功能。
var ymatrixDf = spark.read.format("its-ymatrix").options(options).load()
// 打印分隔线。
println("\r\n*************************************************")
// 打印预期说明。
println("Should display 20 rows from uuid_seq table")
// 打印分隔线。
println("*************************************************")
// 显示读取到的数据。
ymatrixDf.show(false)
// 执行一次 count，确保能完成行动算子。
ymatrixDf.count()

// 打印分隔线。
println("\r\n*************************************************")
// 打印预期说明。
println("Should copy uuid_seq table into uuid_seq_2 recreating the later")
// 打印分隔线。
println("*************************************************")
// 用 overwrite 把 uuid_seq 复制到 uuid_seq_2。
ymatrixDf.write.format("its-ymatrix").options(options.updated("dbtable","uuid_seq_2")).mode("overwrite").save()

// 打印分隔线。
println("\r\n*************************************************")
// 打印预期说明。
println("Should copy uuid_seq table into uuid_seq_2 recreating the later")
// 打印分隔线。
println("*************************************************")
// 再做一次带 repartition 的 overwrite 复制，覆盖 uuid_seq_2。
ymatrixDf.repartition(7).write.format("its-ymatrix").options(options.updated("dbtable","uuid_seq_2")).mode("overwrite").save()

// 下面这行是一个备用示例：按 created_d 重分区后再写。
//ymatrixDf.repartition(col("created_d")).write.format("its-ymatrix").options(options.updated("dbtable","uuid_seq_2")).mode("overwrite").save()

// 读取一个空结果集，用于测试空表 overwrite 的行为。
ymatrixDf = spark.read.format("its-ymatrix").options(options.updated("dbtable","select * from uuid_seq where seq_no < 0")).load()
// 打印分隔线。
println("\r\n*************************************************")
// 打印预期说明。
println("Should display empty rowset und recreate uuid_seq_3 empty table")
// 打印分隔线。
println("*************************************************")
// 展示空结果集。
ymatrixDf.show(false)
// 对空结果集执行 count。
ymatrixDf.count()
// 用空结果集 overwrite uuid_seq_3。
ymatrixDf.write.format("its-ymatrix").options(options.updated("dbtable","uuid_seq_3")).mode("overwrite").save()

// 读取 uuid_seq 的 1 行数据，用于测试 append 到新表。
ymatrixDf = spark.read.format("its-ymatrix").options(options.updated("dbtable","SELECT * FROM uuid_seq limit 1")).load()
// 打印分隔线。
println("\r\n*************************************************")
// 打印预期说明。
println("Should display 1 row and append it to uuid_seq_4 table")
// 打印分隔线。
println("*************************************************")
// 展示这 1 行数据。
ymatrixDf.show(false)
// 对结果执行 count。
ymatrixDf.count()
// 把这 1 行追加写入 uuid_seq_4。
ymatrixDf.write.format("its-ymatrix").options(options.updated("dbtable","uuid_seq_4")).mode("append").save()

// 执行循环读取测试。
LoopRead(options).run()
// 执行循环写入测试。
LoopWrite(options).run()

// 打印结束分隔线。
println("\r\n*************************************************")
// 打印完成提示。
println("Done!")
// 打印结束分隔线。
println("*************************************************")
