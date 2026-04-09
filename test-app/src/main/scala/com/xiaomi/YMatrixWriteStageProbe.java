package com.xiaomi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;

/**
 * 一个开箱即用的单文件探针程序。
 *
 * 目的：
 * 1. 把客户“写入慢”拆成明确阶段
 * 2. 重点区分是不是慢在 save() 本身
 * 3. 避免用 connector 再做 read 验证，防止把读路径耗时混进来
 *
 * 建议：
 * 1. 先只看 write_ymatrix 阶段
 * 2. verify_target_count_jdbc 只做结果校验，不代表 connector 写入耗时
 * 3. 如果客户只关心 save()，可以把 ENABLE_VERIFY 改成 false
 */
public class YMatrixWriteStageProbe {

    private static final Logger LOG = Logger.getLogger(YMatrixWriteStageProbe.class);

    // ==================== 用户可修改配置区 ====================
    // 必改项：如果客户环境和这里不同，请优先修改这几项。
    // 1. CONFIG_LOCAL_IP：建议填写当前运行机器可被 YMatrix 访问到的真实 IP。
    //    如果留空，会自动取本机 IP；若自动获取不对，请手动写死。
    private static final String CONFIG_LOCAL_IP = "";
    // 2. CONFIG_YMATRIX_URL：改成客户自己的 JDBC 地址。
    // 3. CONFIG_YMATRIX_USER / CONFIG_YMATRIX_PASSWORD：改成客户自己的账号密码。
    // 4. CONFIG_YMATRIX_SCHEMA / CONFIG_YMATRIX_TARGET_TABLE：改成客户要写入的 schema 和表名。
    private static final String CONFIG_WAREHOUSE = "/tmp/iceberg_warehouse_stage_probe";
    private static final String CONFIG_ICEBERG_NAMESPACE = "test_db";
    private static final String CONFIG_ICEBERG_TABLE = "yamtrix_test";
    private static final String CONFIG_YMATRIX_URL = "jdbc:postgresql://172.16.100.62:5432/zhangchen";
    private static final String CONFIG_YMATRIX_USER = "admin";
    private static final String CONFIG_YMATRIX_PASSWORD = "YMatrix@123";
    private static final String CONFIG_YMATRIX_SCHEMA = "public";
    private static final String CONFIG_YMATRIX_TARGET_TABLE = "iceberg_yamtrix_probe_test";
    // 一般不用改：只有客户环境端口或超时策略特殊时才调整。
    private static final String CONFIG_SERVER_PORT = "43000";
    private static final String CONFIG_NETWORK_TIMEOUT = "60s";
    private static final String CONFIG_SERVER_TIMEOUT = "60s";
    private static final String CONFIG_DBMESSAGES = "WARN";
    // 一般不用改：当前 demo 只有 3 行数据，保持 1 即可。
    private static final int CONFIG_WRITE_PARTITIONS = 1;
    // 按需修改：
    // CONFIG_ENABLE_VERIFY=true：写完后用 JDBC 校验目标表总行数。
    // CONFIG_SHOW_DATAFRAME=true：打印源数据内容，默认关闭以免输出过多。
    // CONFIG_VERBOSE=true：打印每个细分阶段日志，默认关闭，只输出重点总结。
    // CONFIG_CONNECTOR_LOG_INFO=true：打开 connector 关键 INFO 日志，便于定位写入等待发生在哪一段。
    // CONFIG_DETAIL_TIMING=true：让 connector 额外打印 driver/executor 的细粒度耗时拆解。
    private static final boolean CONFIG_ENABLE_VERIFY = true;
    private static final boolean CONFIG_SHOW_DATAFRAME = false;
    private static final boolean CONFIG_VERBOSE = false;
    private static final boolean CONFIG_CONNECTOR_LOG_INFO = true;
    private static final boolean CONFIG_DETAIL_TIMING = true;
    // ==================== 用户可修改配置区结束 ====================

    private interface StageAction<T> {
        T run() throws Exception;
    }

    private static final class StageTimer {
        private final boolean verbose;
        private final Map<String, Long> stageCosts = new LinkedHashMap<>();

        private StageTimer(boolean verbose) {
            this.verbose = verbose;
        }

        <T> T run(String stageName, StageAction<T> action) throws Exception {
            long startMs = System.currentTimeMillis();
            if (verbose) {
                System.out.println("[stage-start] " + stageName + " time=" + fmtTs(startMs));
            }
            try {
                T result = action.run();
                long endMs = System.currentTimeMillis();
                long costMs = endMs - startMs;
                stageCosts.put(stageName, costMs);
                if (verbose) {
                    System.out.println("[stage-end] " + stageName + " costMs=" + costMs);
                }
                return result;
            } catch (Exception e) {
                long failMs = System.currentTimeMillis();
                long costMs = failMs - startMs;
                stageCosts.put(stageName, costMs);
                System.out.println("[stage-fail] " + stageName + " costMs=" + costMs
                        + " error=" + e.getClass().getName() + ": " + e.getMessage());
                throw e;
            }
        }

        long getCost(String stageName) {
            Long cost = stageCosts.get(stageName);
            return cost == null ? -1L : cost;
        }
    }

    public static void main(String[] args) throws Exception {
        final boolean showDataFrame = CONFIG_SHOW_DATAFRAME;
        final boolean verbose = CONFIG_VERBOSE;
        StageTimer timer = new StageTimer(verbose);
        configureLogging(CONFIG_CONNECTOR_LOG_INFO);

        final String localIp = blankToDefault(CONFIG_LOCAL_IP, resolveLocalIp());
        final String warehouse = CONFIG_WAREHOUSE;
        final String icebergNamespace = CONFIG_ICEBERG_NAMESPACE;
        final String icebergTable = CONFIG_ICEBERG_TABLE;
        final String ymatrixUrl = CONFIG_YMATRIX_URL;
        final String ymatrixUser = CONFIG_YMATRIX_USER;
        final String ymatrixPassword = CONFIG_YMATRIX_PASSWORD;
        final String ymatrixSchema = CONFIG_YMATRIX_SCHEMA;
        final String ymatrixTargetTable = CONFIG_YMATRIX_TARGET_TABLE;
        final String serverPort = CONFIG_SERVER_PORT;
        final String networkTimeout = CONFIG_NETWORK_TIMEOUT;
        final String serverTimeout = CONFIG_SERVER_TIMEOUT;
        final String dbMessages = CONFIG_DBMESSAGES;
        final int writePartitions = CONFIG_WRITE_PARTITIONS;
        final boolean enableVerify = CONFIG_ENABLE_VERIFY;
        final boolean detailTiming = CONFIG_DETAIL_TIMING;

        printConfig(
                localIp, warehouse, icebergNamespace, icebergTable,
                ymatrixUrl, ymatrixUser, ymatrixSchema, ymatrixTargetTable,
                serverPort, networkTimeout, serverTimeout, dbMessages, writePartitions, enableVerify, showDataFrame, verbose
        );

        long appStartMs = System.currentTimeMillis();

        SparkSession spark = timer.run("create_spark_session", () ->
                SparkSession.builder()
                        .master("local[*]")
                        .appName("YMatrixWriteStageProbe")
                        .config("spark.driver.host", localIp)
                        .config("spark.driver.bindAddress", "0.0.0.0")
                        .config("spark.local.ip", localIp)
                        .config("spark.sql.shuffle.partitions", "1")
                        .config("spark.default.parallelism", "1")
                        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
                        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
                        .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse)
                        .getOrCreate()
        );

        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> srcDf = timer.run("build_source_dataframe", () ->
                    spark.createDataFrame(
                                    Arrays.asList(
                                            new OrderRow(1L, "u001", new BigDecimal("18.50"), Timestamp.valueOf("2026-03-19 10:00:00")),
                                            new OrderRow(2L, "u002", new BigDecimal("20.00"), Timestamp.valueOf("2026-03-19 10:05:00")),
                                            new OrderRow(3L, "u003", new BigDecimal("99.99"), Timestamp.valueOf("2026-03-19 10:10:00"))
                                    ),
                                    OrderRow.class
                            )
                            .select(
                                    col("orderId").as("order_id"),
                                    col("userId").as("user_id"),
                                    col("amount"),
                                    col("createdAt").as("created_at")
                            )
            );

            final String fullIcebergTable = "iceberg_catalog." + icebergNamespace + "." + icebergTable;

            timer.run("create_iceberg_namespace", () -> {
                spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog." + icebergNamespace);
                return null;
            });

            timer.run("write_iceberg_create_or_replace", () -> {
                srcDf.writeTo(fullIcebergTable).createOrReplace();
                return null;
            });

            Dataset<Row> icebergDf = timer.run("load_iceberg_dataframe", () -> spark.sql("SELECT * FROM " + fullIcebergTable));

            if (showDataFrame) {
                timer.run("show_iceberg_dataframe", () -> {
                    icebergDf.show(false);
                    return null;
                });
            }

            Long sourceCount = timer.run("count_iceberg_dataframe", icebergDf::count);
            System.out.println("[metric] source_row_count=" + sourceCount);

            timer.run("prepare_write_dataframe", () -> {
                Dataset<Row> prepared = icebergDf.repartition(writePartitions);
                if (verbose) {
                    System.out.println("[metric] write_partitions=" + prepared.rdd().getNumPartitions());
                }
                return prepared;
            });

            Dataset<Row> writeDf = icebergDf.repartition(writePartitions);

            timer.run("write_ymatrix", () -> {
                writeDf.write()
                        .format("its-ymatrix")
                        .option("url", ymatrixUrl)
                        .option("user", ymatrixUser)
                        .option("password", ymatrixPassword)
                        .option("dbschema", ymatrixSchema)
                        .option("dbtable", ymatrixTargetTable)
                        .option("distributedby", "order_id")
                        .option("server.port", serverPort)
                        .option("network.timeout", networkTimeout)
                        .option("server.timeout", serverTimeout)
                        .option("dbmessages", dbMessages)
                        .option("timing.detail", String.valueOf(detailTiming))
                        .mode("append")
                        .save();
                return null;
            });

            if (enableVerify) {
                Long targetCount = timer.run("verify_target_count_jdbc", () ->
                        queryCountByJdbc(ymatrixUrl, ymatrixUser, ymatrixPassword, ymatrixSchema, ymatrixTargetTable)
                );
                System.out.println("[metric] target_row_count=" + targetCount);
            }

            long appEndMs = System.currentTimeMillis();
            printSummary(timer, ymatrixSchema, ymatrixTargetTable, appEndMs - appStartMs, showDataFrame, enableVerify);
        } finally {
            timer.run("stop_spark", () -> {
                spark.stop();
                return null;
            });
        }
    }

    private static Long queryCountByJdbc(
            String url,
            String user,
            String password,
            String schema,
            String table
    ) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        try (Connection conn = DriverManager.getConnection(url, props);
             PreparedStatement stmt = conn.prepareStatement(
                     "select count(*) as row_count from " + schema + "." + table
             );
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void printConfig(
            String localIp,
            String warehouse,
            String icebergNamespace,
            String icebergTable,
            String ymatrixUrl,
            String ymatrixUser,
            String ymatrixSchema,
            String ymatrixTargetTable,
            String serverPort,
            String networkTimeout,
            String serverTimeout,
            String dbMessages,
            int writePartitions,
            boolean enableVerify,
            boolean showDataFrame,
            boolean verbose
    ) {
        if (!verbose) {
            return;
        }
        LOG.warn("[config] localIp=" + localIp);
        LOG.warn("[config] warehouse=" + warehouse);
        LOG.warn("[config] icebergTable=iceberg_catalog." + icebergNamespace + "." + icebergTable);
        LOG.warn("[config] ymatrixUrl=" + ymatrixUrl);
        LOG.warn("[config] ymatrixUser=" + ymatrixUser);
        LOG.warn("[config] ymatrixTarget=" + ymatrixSchema + "." + ymatrixTargetTable);
        LOG.warn("[config] serverPort=" + serverPort);
        LOG.warn("[config] networkTimeout=" + networkTimeout);
        LOG.warn("[config] serverTimeout=" + serverTimeout);
        LOG.warn("[config] dbMessages=" + dbMessages);
        LOG.warn("[config] writePartitions=" + writePartitions);
        LOG.warn("[config] enableVerify=" + enableVerify);
        LOG.warn("[config] showDataFrame=" + showDataFrame);
        LOG.warn("[config] verbose=" + verbose);
    }

    private static void configureLogging(boolean connectorLogInfo) {
        Logger.getRootLogger().setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.ui").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty").setLevel(Level.WARN);
        Logger.getLogger("org.apache.parquet").setLevel(Level.ERROR);
        Logger.getLogger("parquet").setLevel(Level.ERROR);

        if (connectorLogInfo) {
            Logger.getLogger("com.itsumma.gpconnector").setLevel(Level.INFO);
            Logger.getLogger("com.itsumma.gpconnector.GPClient").setLevel(Level.INFO);
            Logger.getLogger("com.itsumma.gpconnector.writer").setLevel(Level.INFO);
            Logger.getLogger("com.itsumma.gpconnector.reader").setLevel(Level.INFO);
            Logger.getLogger("com.itsumma.gpconnector.rmi").setLevel(Level.INFO);
            Logger.getLogger("com.itsumma.gpconnector.gpfdist").setLevel(Level.INFO);
        }
    }

    private static void printSummary(
            StageTimer timer,
            String ymatrixSchema,
            String ymatrixTargetTable,
            long totalCostMs,
            boolean showDataFrame,
            boolean enableVerify
    ) {
        System.out.println("[focus] write_ymatrix_ms=" + timer.getCost("write_ymatrix"));
        System.out.println("[focus] total_ms=" + totalCostMs);
        System.out.println("[focus] spark_startup_ms=" + timer.getCost("create_spark_session"));
        System.out.println("[focus] iceberg_prepare_ms="
                + sum(
                timer.getCost("build_source_dataframe"),
                timer.getCost("create_iceberg_namespace"),
                timer.getCost("write_iceberg_create_or_replace"),
                timer.getCost("load_iceberg_dataframe"),
                showDataFrame ? timer.getCost("show_iceberg_dataframe") : 0L,
                timer.getCost("count_iceberg_dataframe"),
                timer.getCost("prepare_write_dataframe")
        ));
        if (enableVerify) {
            System.out.println("[focus] verify_count_ms=" + timer.getCost("verify_target_count_jdbc"));
        }
        System.out.println("[result] ymatrix_table=" + ymatrixSchema + "." + ymatrixTargetTable);
    }

    private static long sum(long... values) {
        long total = 0L;
        for (long value : values) {
            if (value > 0) {
                total += value;
            }
        }
        return total;
    }

    private static String blankToDefault(String value, String defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static String resolveLocalIp() throws Exception {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private static String fmtTs(long ms) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(ms));
    }

    public static class OrderRow {
        private Long orderId;
        private String userId;
        private BigDecimal amount;
        private Timestamp createdAt;

        public OrderRow() {
        }

        public OrderRow(Long orderId, String userId, BigDecimal amount, Timestamp createdAt) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.createdAt = createdAt;
        }

        public Long getOrderId() {
            return orderId;
        }

        public void setOrderId(Long orderId) {
            this.orderId = orderId;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public BigDecimal getAmount() {
            return amount;
        }

        public void setAmount(BigDecimal amount) {
            this.amount = amount;
        }

        public Timestamp getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Timestamp createdAt) {
            this.createdAt = createdAt;
        }
    }
}
