package com.xiaomi;

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

    private static final String DEFAULT_WAREHOUSE = "/tmp/iceberg_warehouse_stage_probe";
    private static final String DEFAULT_NAMESPACE = "test_db";
    private static final String DEFAULT_ICEBERG_TABLE = "yamtrix_test";
    private static final String DEFAULT_TARGET_SCHEMA = "public";
    private static final String DEFAULT_TARGET_TABLE_PREFIX = "iceberg_yamtrix_probe";
    private static final String DEFAULT_URL = "jdbc:postgresql://172.16.100.62:5432/zhangchen";
    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "YMatrix@123";
    private static final String DEFAULT_SERVER_PORT = "43000";
    private static final String DEFAULT_NETWORK_TIMEOUT = "60s";
    private static final String DEFAULT_SERVER_TIMEOUT = "60s";
    private static final String DEFAULT_DBMESSAGES = "WARN";
    private static final String DEFAULT_DISTRIBUTED_BY = "order_id";
    private static final int DEFAULT_WRITE_PARTITIONS = 1;
    private static final boolean DEFAULT_ENABLE_VERIFY = true;

    private interface StageAction<T> {
        T run() throws Exception;
    }

    private static final class StageTimer {
        <T> T run(String stageName, StageAction<T> action) throws Exception {
            long startMs = System.currentTimeMillis();
            System.out.println("[stage-start] " + stageName + " at=" + startMs + " ts=" + fmtTs(startMs));
            try {
                T result = action.run();
                long endMs = System.currentTimeMillis();
                System.out.println("[stage-end] " + stageName + " at=" + endMs + " costMs=" + (endMs - startMs));
                return result;
            } catch (Exception e) {
                long failMs = System.currentTimeMillis();
                System.out.println("[stage-fail] " + stageName + " at=" + failMs + " costMs=" + (failMs - startMs)
                        + " error=" + e.getClass().getName() + ": " + e.getMessage());
                throw e;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StageTimer timer = new StageTimer();

        final String localIp = envOrDefault("SPARK_LOCAL_IP", resolveLocalIp());
        final String warehouse = envOrDefault("ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE);
        final String icebergNamespace = envOrDefault("ICEBERG_NAMESPACE", DEFAULT_NAMESPACE);
        final String icebergTable = envOrDefault("ICEBERG_TABLE", DEFAULT_ICEBERG_TABLE);
        final String ymatrixUrl = envOrDefault("YMATRIX_URL", DEFAULT_URL);
        final String ymatrixUser = envOrDefault("YMATRIX_USER", DEFAULT_USER);
        final String ymatrixPassword = envOrDefault("YMATRIX_PASSWORD", DEFAULT_PASSWORD);
        final String ymatrixSchema = envOrDefault("YMATRIX_SCHEMA", DEFAULT_TARGET_SCHEMA);
        final String ymatrixTargetTable = envOrDefault(
                "YMATRIX_TARGET_TABLE",
                DEFAULT_TARGET_TABLE_PREFIX + "_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())
        );
        final String serverPort = envOrDefault("YMATRIX_SERVER_PORT", DEFAULT_SERVER_PORT);
        final String networkTimeout = envOrDefault("YMATRIX_NETWORK_TIMEOUT", DEFAULT_NETWORK_TIMEOUT);
        final String serverTimeout = envOrDefault("YMATRIX_SERVER_TIMEOUT", DEFAULT_SERVER_TIMEOUT);
        final String dbMessages = envOrDefault("YMATRIX_DBMESSAGES", DEFAULT_DBMESSAGES);
        final String distributedBy = envOrDefault("YMATRIX_DISTRIBUTED_BY", DEFAULT_DISTRIBUTED_BY);
        final int writePartitions = Integer.parseInt(envOrDefault("YMATRIX_WRITE_PARTITIONS", String.valueOf(DEFAULT_WRITE_PARTITIONS)));
        final boolean enableVerify = Boolean.parseBoolean(envOrDefault("ENABLE_VERIFY", String.valueOf(DEFAULT_ENABLE_VERIFY)));

        printConfig(
                localIp, warehouse, icebergNamespace, icebergTable,
                ymatrixUrl, ymatrixUser, ymatrixSchema, ymatrixTargetTable,
                serverPort, networkTimeout, serverTimeout, dbMessages, distributedBy, writePartitions, enableVerify
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

            timer.run("show_iceberg_dataframe", () -> {
                icebergDf.show(false);
                return null;
            });

            Long sourceCount = timer.run("count_iceberg_dataframe", icebergDf::count);
            System.out.println("[metric] iceberg_row_count=" + sourceCount);

            boolean hasSourceRows = timer.run("check_source_dataframe_non_empty", () ->
                    !icebergDf.limit(1).takeAsList(1).isEmpty()
            );
            System.out.println("[metric] source_non_empty=" + hasSourceRows);

            if (hasSourceRows) {
                Dataset<Row> writeDf = timer.run("prepare_write_dataframe", () -> {
                    Dataset<Row> prepared = icebergDf.repartition(writePartitions, col(distributedBy));
                    System.out.println("[metric] write_partitions=" + prepared.rdd().getNumPartitions());
                    return prepared;
                });

                timer.run("write_ymatrix", () -> {
                    writeDf.write()
                            .format("its-ymatrix")
                            .option("url", ymatrixUrl)
                            .option("user", ymatrixUser)
                            .option("password", ymatrixPassword)
                            .option("dbschema", ymatrixSchema)
                            .option("dbtable", ymatrixTargetTable)
                            .option("distributedby", distributedBy)
                            .option("server.port", serverPort)
                            .option("network.timeout", networkTimeout)
                            .option("server.timeout", serverTimeout)
                            .option("dbmessages", dbMessages)
                            .mode("append")
                            .save();
                    return null;
                });
            } else {
                System.out.println("[stage-skip] write_ymatrix reason=source_dataframe_empty");
            }

            if (enableVerify) {
                Long targetCount = timer.run("verify_target_count_jdbc", () ->
                        queryCountByJdbc(ymatrixUrl, ymatrixUser, ymatrixPassword, ymatrixSchema, ymatrixTargetTable)
                );
                System.out.println("[metric] target_row_count=" + targetCount);
            }

            long appEndMs = System.currentTimeMillis();
            System.out.println("[result] ymatrix_table=" + ymatrixSchema + "." + ymatrixTargetTable);
            System.out.println("[result] total_cost_ms=" + (appEndMs - appStartMs));
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
            String distributedBy,
            int writePartitions,
            boolean enableVerify
    ) {
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
        LOG.warn("[config] distributedBy=" + distributedBy);
        LOG.warn("[config] writePartitions=" + writePartitions);
        LOG.warn("[config] enableVerify=" + enableVerify);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
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
