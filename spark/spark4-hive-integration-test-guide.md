# Spark4 与 Hive3/Hive4 集成测试指南

> 文档版本: v2.0 | 日期: 2026-07-03 | 适用版本: Spark 4.1.2 + Hive 3.1.3 / Hive 4.2.0 + Iceberg 1.6 + Paimon 0.8.2

> ⚠️ **重要提醒**: 本版本组合存在已知兼容性风险。**Paimon 0.8.2 与 Spark 4.1.2 的组合风险等级为 CRITICAL**（Paimon 0.8.x 无官方 Spark 4 模块），Iceberg 1.6 与 Hive 4.2.0 存在 ClassLoader 版本冲突风险。详细分析与规避方案请先阅读 **[附录 D: 版本兼容性风险分析](#附录-d-版本兼容性风险分析与重点测试建议)**。建议优先执行 D.8 节列出的 P0 优先级测试。

---

## 目录

1. [测试环境概述](#1-测试环境概述)
2. [Spark4 配置与 Hive 集成](#2-spark4-配置与-hive-集成)
3. [Parquet 格式测试](#3-parquet-格式测试)
4. [ORC 格式测试](#4-orc-格式测试)
5. [内部表 (Managed Table) 测试](#5-内部表-managed-table-测试)
6. [外部表 (External Table) 测试](#6-外部表-external-table-测试)
7. [Iceberg 表测试](#7-iceberg-表测试)
8. [Paimon 表测试](#8-paimon-表测试)
9. [混合场景与边界测试](#9-混合场景与边界测试)
10. [性能基准参考](#10-性能基准参考)

---

## 1. 测试环境概述

### 1.1 环境版本矩阵

| 组件       | 版本             | 说明                       |
| ---------- | ---------------- | -------------------------- |
| Spark      | 4.0.0+           | 主要测试引擎               |
| Hive       | 3.1.3 / 4.0.0    | Metastore + 执行引擎       |
| Hadoop     | 3.3.6+           | 底层存储 (HDFS / S3 / OSS) |
| Iceberg    | 1.5.0+           | 表格式 (需 Hive 4 原生支持) |
| Paimon     | 0.9.0+           | 湖仓表格式                 |
| Java       | 17 或 21         | Spark4 要求 Java 17+       |
| Scala      | 2.13 或 3.x      | Spark4 默认 Scala 2.13     |

### 1.2 测试数据集准备

```sql
-- ============================================================
-- 1.2.1 创建基准测试数据库
-- ============================================================
CREATE DATABASE IF NOT EXISTS spark4_test_db
  LOCATION '/warehouse/spark4_test_db.db';
USE spark4_test_db;

-- ============================================================
-- 1.2.2 生成测试数据 (Spark 端执行)
-- ============================================================

-- 小型数据集 (1万行) - 用于快速功能验证
CREATE OR REPLACE TEMP VIEW test_data_10k AS
SELECT
  id,
  concat('name_', cast(id AS STRING))                     AS name,
  cast(rand(id * 7) * 100000 AS DECIMAL(18,2))            AS amount,
  cast(from_unixtime(1700000000 + id * 3600) AS TIMESTAMP) AS create_time,
  date_add('2024-01-01', cast(rand(id * 13) * 365 AS INT)) AS biz_date,
  CASE WHEN rand(id * 17) > 0.5 THEN true ELSE false END  AS is_active,
  cast(rand(id * 19) * 100 AS BIGINT)                     AS score,
  concat('category_', cast(cast(rand(id * 23) * 10 AS INT) AS STRING)) AS category,
  cast(rand(id * 29) * 1000 AS DOUBLE)                    AS metric_double,
  cast(rand(id * 31) * 100 AS FLOAT)                      AS metric_float
FROM range(1, 10001);

-- 大型数据集 (1千万行) - 用于性能和分区测试
CREATE OR REPLACE TEMP VIEW test_data_10m AS
SELECT
  id,
  concat('name_', cast(id AS STRING))                     AS name,
  cast(rand(id * 7) * 100000 AS DECIMAL(18,2))            AS amount,
  cast(from_unixtime(1700000000 + id * 3600) AS TIMESTAMP) AS create_time,
  date_add('2024-01-01', cast(rand(id * 13) * 365 AS INT)) AS biz_date,
  CASE WHEN rand(id * 17) > 0.5 THEN true ELSE false END  AS is_active,
  cast(rand(id * 19) * 100 AS BIGINT)                     AS score,
  concat('category_', cast(cast(rand(id * 23) * 100 AS INT) AS STRING)) AS category,
  cast(rand(id * 29) * 1000 AS DOUBLE)                    AS metric_double,
  cast(rand(id * 31) * 100 AS FLOAT)                      AS metric_float
FROM range(1, 10000001);
```

---

## 2. Spark4 配置与 Hive 集成

### 2.1 Spark4 连接 Hive3

#### 2.1.1 spark-defaults.conf 关键配置

```properties
# ============================================================
# Spark4 on Hive3 核心配置
# 文件: $SPARK_HOME/conf/spark-defaults.conf
# ============================================================

# --- Hive Metastore 连接 ---
spark.sql.catalogImplementation                hive
spark.hadoop.hive.metastore.uris               thrift://hive-metastore-host:9083
spark.hadoop.hive.metastore.warehouse.dir       /warehouse

# --- Hive 客户端版本 ---
spark.sql.hive.metastore.version               3.1
spark.sql.hive.metastore.jars                  maven
# 若使用已下载的 jar 包:
# spark.sql.hive.metastore.jars.path           /opt/spark/jars/hive-3.1/*

# --- Hive 执行引擎 (让 Hive 端也用 Spark4 执行) ---
spark.hadoop.hive.execution.engine             spark

# --- HDFS 配置 ---
spark.hadoop.fs.defaultFS                      hdfs://namenode:8020

# --- Kerberos (如需要) ---
# spark.hadoop.hive.metastore.sasl.enabled     true
# spark.hadoop.hive.metastore.kerberos.principal  hive/_HOST@REALM.COM
# spark.hadoop.hive.metastore.kerberos.keytab.file  /etc/security/keytabs/hive.keytab

# --- 序列化 ---
spark.hadoop.hive.metastore.schema.verification  false
spark.hadoop.hive.metastore.schema.verification.record.version  false
spark.hadoop.datanucleus.autoCreateSchema        false

# --- Thrift Server (如使用 Spark Thrift Server 替代 HiveServer2) ---
spark.sql.thriftServer.queryTimeout              3600s
```

#### 2.1.2 hive-site.xml (放置于 $SPARK_HOME/conf/)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- Metastore 连接 -->
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://hive-metastore-host:9083</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/warehouse</value>
    </property>

    <!-- HiveServer2 连接 (Spark Thrift 模式) -->
    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>
    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>0.0.0.0</value>
    </property>

    <!-- 执行引擎 -->
    <property>
        <name>hive.execution.engine</name>
        <value>spark</value>
    </property>

    <!-- 动态分区 -->
    <property>
        <name>hive.exec.dynamic.partition</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.exec.dynamic.partition.mode</name>
        <value>nonstrict</value>
    </property>

    <!-- 事务与 ACID (Hive3) -->
    <property>
        <name>hive.support.concurrency</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.txn.manager</name>
        <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
    </property>
    <property>
        <name>hive.compactor.initiator.on</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.compactor.worker.threads</name>
        <value>2</value>
    </property>
</configuration>
```

### 2.2 Spark4 连接 Hive4

#### 2.2.1 spark-defaults.conf (Hive4 差异配置)

```properties
# ============================================================
# Spark4 on Hive4 核心配置 (差异部分)
# ============================================================

spark.sql.hive.metastore.version               4.0
spark.sql.hive.metastore.jars                  maven

# Hive4 原生支持 Iceberg, 无需额外 catalog 配置 metastore 层
spark.sql.extensions                           org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

# Hive4 引入的新配置
spark.hadoop.metastore.storage.schema.reader.impl  org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader
spark.hadoop.hive.metastore.event.db.notification.api.auth  false
```

#### 2.2.2 Hive4 新增特性相关配置

```properties
# --- Iceberg (Hive4 原生 Metastore 集成) ---
spark.sql.catalog.iceberg_catalog              org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type         hive
spark.sql.catalog.iceberg_catalog.uri          thrift://hive-metastore-host:9083

# --- Paimon ---
spark.sql.catalog.paimon_catalog               org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon_catalog.warehouse     /warehouse/paimon

# --- Hive4 Warehouse Connector ---
spark.hadoop.hive.warehouse.connector.port     9083
```

### 2.3 SparkSession 编程构建 (代码方式)

```scala
// ============================================================
// 2.3.1 Spark4 + Hive3
// ============================================================
import org.apache.spark.sql.SparkSession

val spark3 = SparkSession.builder()
  .appName("Spark4-Hive3-Integration-Test")
  .master("yarn")                              // 或 "local[*]" 用于本地测试
  .config("spark.sql.catalogImplementation", "hive")
  .config("spark.sql.hive.metastore.version", "3.1")
  .config("spark.sql.hive.metastore.jars", "maven")
  .config("hive.metastore.uris", "thrift://hive-metastore-host:9083")
  .config("spark.sql.warehouse.dir", "/warehouse")
  .config("spark.sql.adaptive.enabled", "true")
  .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
  .enableHiveSupport()
  .getOrCreate()

// ============================================================
// 2.3.2 Spark4 + Hive4 (含 Iceberg/Paimon)
// ⚠️ Paimon 0.8.2 无 spark-4.0 模块, 见附录 D 风险分析
// ============================================================
val spark4 = SparkSession.builder()
  .appName("Spark4-Hive4-Integration-Test")
  .master("yarn")
  .config("spark.sql.catalogImplementation", "hive")
  .config("spark.sql.hive.metastore.version", "4.0")
  .config("spark.sql.hive.metastore.jars", "maven")
  .config("hive.metastore.uris", "thrift://hive-metastore-host:9083")
  .config("spark.sql.warehouse.dir", "/warehouse")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
    "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
  // Iceberg Catalog (Iceberg 1.6 → 建议使用 iceberg-spark-runtime-4.0_2.13)
  .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.iceberg_catalog.type", "hive")
  .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://hive-metastore-host:9083")
  // Paimon Catalog (⚠️ Paimon 0.8.2 可能无法在 Spark4 工作, 建议升级 0.9+)
  .config("spark.sql.catalog.paimon_catalog", "org.apache.paimon.spark.SparkCatalog")
  .config("spark.sql.catalog.paimon_catalog.warehouse", "/warehouse/paimon")
  .enableHiveSupport()
  .getOrCreate()
```

### 2.4 启动测试会话 (CLI 方式)

```bash
# ============================================================
# 方式 A: Spark SQL Shell 连接 Hive3
# ============================================================
spark-sql \
  --master yarn \
  --conf spark.sql.hive.metastore.version=3.1 \
  --conf spark.sql.hive.metastore.jars=maven \
  --conf hive.metastore.uris=thrift://hive-metastore-host:9083 \
  --conf spark.sql.warehouse.dir=/warehouse

# ============================================================
# 方式 B: Spark Shell (Scala) 连接 Hive4
# ============================================================
spark-shell \
  --master yarn \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.2,\
org.apache.paimon:paimon-spark-3.5:0.9.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg_catalog.type=hive \
  --conf spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore-host:9083 \
  --conf spark.sql.catalog.paimon_catalog=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon_catalog.warehouse=/warehouse/paimon

# ============================================================
# 方式 C: Beeline 连接 Spark Thrift Server
# ============================================================
beeline -u jdbc:hive2://spark-thrift-host:10000/spark4_test_db
```

### 2.5 连接验证 SQL

```sql
-- ============================================================
-- 2.5.1 验证 Metastore 连通性
-- ============================================================
SHOW DATABASES;
DESCRIBE DATABASE EXTENDED spark4_test_db;

-- ============================================================
-- 2.5.2 验证 Spark 版本
-- ============================================================
SELECT version();

-- ============================================================
-- 2.5.3 验证配置项
-- ============================================================
SET spark.sql.hive.metastore.version;
SET hive.metastore.uris;
SET spark.sql.catalogImplementation;

-- ============================================================
-- 2.5.4 验证已注册 Catalog (Hive4 模式)
-- ============================================================
SHOW CATALOGS;
-- 预期输出: spark_catalog, iceberg_catalog, paimon_catalog

-- ============================================================
-- 2.5.5 验证 Iceberg/Paimon 扩展是否加载
-- ============================================================
SELECT
  class_name,
  extension_type
FROM
  spark.sql.extensions.system_table;
-- 或通过 SHOW 命令检查扩展 SQL 语法是否可用
```

---

## 3. Parquet 格式测试

### 3.1 测试矩阵

| 测试项      | 压缩编码   | 说明                   |
| -------- | ------ | -------------------- |
| 读写       | none   | 无压缩                  |
| 读写       | snappy | 默认推荐压缩 (速度优先)        |
| 读写       | gzip   | 高压缩比 (存储优先)          |
| 读写       | lz4    | 高速压缩                 |
| 读写       | zstd   | 新一代平衡压缩 (推荐)         |
| 读写       | lzo    | 需额外安装 native lib     |
| 读写       | brotli | 高压缩比 (Parquet 1.12+) |
| 分区读写     | zstd   | 分区表 + 压缩组合           |
| 复杂类型     | snappy | array/map/struct 类型  |
| Schema演化 | -      | 列增加/删除/类型变更          |

### 3.2 建表语句 (Hive DDL → 通过 Metastore)

```sql
-- ============================================================
-- 3.2.1 Parquet + Snappy (基准)
-- ============================================================
CREATE DATABASE IF NOT EXISTS spark4_test_db;
USE spark4_test_db;

DROP TABLE IF EXISTS parquet_snappy_test;
CREATE TABLE parquet_snappy_test (
  id              BIGINT        COMMENT '主键ID',
  name            STRING        COMMENT '名称',
  amount          DECIMAL(18,2) COMMENT '金额',
  create_time     TIMESTAMP     COMMENT '创建时间',
  biz_date        DATE          COMMENT '业务日期',
  is_active       BOOLEAN       COMMENT '是否活跃',
  score           BIGINT        COMMENT '评分',
  category        STRING        COMMENT '分类',
  metric_double   DOUBLE        COMMENT '双精度指标',
  metric_float    FLOAT         COMMENT '单精度指标'
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  'parquet.block.size'  = '134217728'   -- 128MB
);

-- ============================================================
-- 3.2.2 Parquet + ZSTD (推荐存储密集场景)
-- ============================================================
DROP TABLE IF EXISTS parquet_zstd_test;
CREATE TABLE parquet_zstd_test LIKE parquet_snappy_test;
ALTER TABLE parquet_zstd_test SET TBLPROPERTIES (
  'parquet.compression' = 'ZSTD',
  'parquet.compression.level' = '3'     -- zstd 压缩级别 1-22
);

-- ============================================================
-- 3.2.3 Parquet + GZIP
-- ============================================================
DROP TABLE IF EXISTS parquet_gzip_test;
CREATE TABLE parquet_gzip_test LIKE parquet_snappy_test;
ALTER TABLE parquet_gzip_test SET TBLPROPERTIES (
  'parquet.compression' = 'GZIP'
);

-- ============================================================
-- 3.2.4 Parquet + LZ4
-- ============================================================
DROP TABLE IF EXISTS parquet_lz4_test;
CREATE TABLE parquet_lz4_test LIKE parquet_snappy_test;
ALTER TABLE parquet_lz4_test SET TBLPROPERTIES (
  'parquet.compression' = 'LZ4'
);

-- ============================================================
-- 3.2.5 Parquet + LZO (需确认 native lib 已安装)
-- ============================================================
DROP TABLE IF EXISTS parquet_lzo_test;
CREATE TABLE parquet_lzo_test LIKE parquet_snappy_test;
ALTER TABLE parquet_lzo_test SET TBLPROPERTIES (
  'parquet.compression' = 'LZO'
);

-- ============================================================
-- 3.2.6 Parquet + Brotli
-- ============================================================
DROP TABLE IF EXISTS parquet_brotli_test;
CREATE TABLE parquet_brotli_test LIKE parquet_snappy_test;
ALTER TABLE parquet_brotli_test SET TBLPROPERTIES (
  'parquet.compression' = 'BROTLI'
);

-- ============================================================
-- 3.2.7 Parquet + 不压缩
-- ============================================================
DROP TABLE IF EXISTS parquet_none_test;
CREATE TABLE parquet_none_test LIKE parquet_snappy_test;
ALTER TABLE parquet_none_test SET TBLPROPERTIES (
  'parquet.compression' = 'UNCOMPRESSED'
);
```

### 3.3 写入测试 (INSERT)

```sql
-- ============================================================
-- 3.3.1 INSERT INTO (追加写入)
-- ============================================================
-- Parquet Snappy - 1万行插入
INSERT INTO parquet_snappy_test
SELECT * FROM test_data_10k;

-- Parquet ZSTD - 1万行插入
INSERT INTO parquet_zstd_test
SELECT * FROM test_data_10k;

-- Parquet GZIP - 1万行插入
INSERT INTO parquet_gzip_test
SELECT * FROM test_data_10k;

-- Parquet LZ4 - 1万行插入
INSERT INTO parquet_lz4_test
SELECT * FROM test_data_10k;

-- Parquet LZO - 1万行插入
INSERT INTO parquet_lzo_test
SELECT * FROM test_data_10k;

-- Parquet Brotli - 1万行插入
INSERT INTO parquet_brotli_test
SELECT * FROM test_data_10k;

-- Parquet None (无压缩)
INSERT INTO parquet_none_test
SELECT * FROM test_data_10k;

-- ============================================================
-- 3.3.2 INSERT OVERWRITE (覆盖写入)
-- ============================================================
INSERT OVERWRITE TABLE parquet_snappy_test
SELECT * FROM test_data_10k;

-- ============================================================
-- 3.3.3 Spark DataFrame 写入 (代码/Scala模式) 
-- ============================================================
-- 在 spark-shell 中执行:
/*
// 从临时视图读取并写入 Parquet 表
spark.table("test_data_10k")
  .write
  .mode("append")
  .format("parquet")
  .option("compression", "zstd")
  .insertInto("parquet_zstd_test")

// 或使用 saveAsTable
spark.table("test_data_10k")
  .write
  .mode("overwrite")
  .format("parquet")
  .option("compression", "snappy")
  .saveAsTable("parquet_snappy_test_v2")
*/
```

### 3.4 读取与查询测试

```sql
-- ============================================================
-- 3.4.1 基础 SELECT + 聚合
-- ============================================================
-- 计数检查
SELECT 'parquet_snappy' AS tbl, count(*) AS row_count FROM parquet_snappy_test
UNION ALL
SELECT 'parquet_zstd', count(*) FROM parquet_zstd_test
UNION ALL
SELECT 'parquet_gzip', count(*) FROM parquet_gzip_test
UNION ALL
SELECT 'parquet_lz4', count(*) FROM parquet_lz4_test
UNION ALL
SELECT 'parquet_lzo', count(*) FROM parquet_lzo_test
UNION ALL
SELECT 'parquet_brotli', count(*) FROM parquet_brotli_test
UNION ALL
SELECT 'parquet_none', count(*) FROM parquet_none_test;

-- ============================================================
-- 3.4.2 聚合查询性能测试
-- ============================================================
SELECT category, count(*) AS cnt, avg(score) AS avg_score
FROM parquet_snappy_test
GROUP BY category
ORDER BY cnt DESC;

-- ============================================================
-- 3.4.3 JOIN 查询
-- ============================================================
SELECT a.id, a.name, b.amount
FROM parquet_snappy_test a
JOIN parquet_zstd_test b ON a.id = b.id
WHERE a.category = 'category_5';

-- ============================================================
-- 3.4.4 谓词下推验证 (EXPLAIN 检查)
-- ============================================================
EXPLAIN EXTENDED
SELECT * FROM parquet_zstd_test
WHERE biz_date = '2024-06-15' AND category = 'category_3';

-- 预期: PushedFilters 应包含 IsNotNull, EqualTo 等

-- ============================================================
-- 3.4.5 Spark DataFrame 读取
-- ============================================================
/*
// spark-shell:
val df = spark.table("parquet_zstd_test")
  .where($"category" === "category_3")
  .groupBy("biz_date")
  .agg(count("*").as("cnt"), avg("score").as("avg_score"))
  .orderBy($"biz_date")

df.explain("extended")  // 查看执行计划
df.show(20)
*/
```

### 3.5 更新与删除测试

```sql
-- ============================================================
-- 3.5.1 UPDATE (Hive ACID 事务表)
-- 注意: Hive3+ 非事务表默认不支持 UPDATE/DELETE
-- 需要将表设置为事务表或使用 Iceberg/Paimon
-- ============================================================

-- 方式 A: Hive ACID 事务表 (需设置 transactional=true)
DROP TABLE IF EXISTS parquet_acid_test;
CREATE TABLE parquet_acid_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'transactional'       = 'true',
  'transactional_properties' = 'insert_only',  -- Hive3 insert_only
  'parquet.compression' = 'ZSTD'
);

INSERT INTO parquet_acid_test SELECT * FROM test_data_10k;

-- Hive ACID 表通过 MERGE 语法更新 (Hive3 MERGE 有限制)
MERGE INTO parquet_acid_test AS target
USING (
  SELECT 1 AS id, CAST('updated_name' AS STRING) AS name
  UNION ALL
  SELECT 2 AS id, CAST('updated_name_2' AS STRING) AS name
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name;

-- 验证更新结果
SELECT id, name FROM parquet_acid_test WHERE id IN (1, 2);

-- DELETE
DELETE FROM parquet_acid_test WHERE id > 9990;

-- 验证删除结果
SELECT count(*) AS remaining_count FROM parquet_acid_test;
-- 预期: 9990

-- ============================================================
-- 3.5.2 Spark DataFrame 覆写方式实现"更新"
-- (非事务表的变通方案)
-- ============================================================
/*
// spark-shell:
import org.apache.spark.sql.functions._

val original = spark.table("parquet_snappy_test")
val updated = original
  .withColumn("name",
    when($"id" === 1, "updated_name")
    .when($"id" === 2, "updated_name_2")
    .otherwise($"name"))

updated.write.mode("overwrite").insertInto("parquet_snappy_test")
*/
```

### 3.6 分区表测试

```sql
-- ============================================================
-- 3.6.1 Parquet 分区表 (按日期 + 分类二级分区)
-- ============================================================
DROP TABLE IF EXISTS parquet_partitioned_test;
CREATE TABLE parquet_partitioned_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  is_active       BOOLEAN,
  score           BIGINT,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
PARTITIONED BY (biz_date DATE, category STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'ZSTD');

-- 静态分区写入
INSERT INTO parquet_partitioned_test
  PARTITION (biz_date = '2024-06-15', category = 'category_3')
SELECT id, name, amount, create_time, is_active, score, metric_double, metric_float
FROM test_data_10k
WHERE id <= 1000;

-- 动态分区写入
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO parquet_partitioned_test
  PARTITION (biz_date, category)
SELECT id, name, amount, create_time, is_active, score, metric_double, metric_float, biz_date, category
FROM test_data_10k;

-- ============================================================
-- 3.6.2 分区裁剪验证
-- ============================================================
EXPLAIN EXTENDED
SELECT count(*)
FROM parquet_partitioned_test
WHERE biz_date = '2024-06-15' AND category = 'category_3';
-- 预期: PartitionFilters 只扫描一个分区

-- ============================================================
-- 3.6.3 分区增删
-- ============================================================
-- 查看分区列表
SHOW PARTITIONS parquet_partitioned_test;

-- 删除指定分区
ALTER TABLE parquet_partitioned_test
  DROP IF EXISTS PARTITION (biz_date = '2024-01-01', category = 'category_0');

-- 添加分区 (外部表场景)
ALTER TABLE parquet_partitioned_test
  ADD PARTITION (biz_date = '2024-12-31', category = 'category_end');

-- 修复分区 (MSCK REPAIR)
MSCK REPAIR TABLE parquet_partitioned_test;
```

### 3.7 复杂数据类型测试

```sql
-- ============================================================
-- 3.7.1 Parquet + 复杂类型 (Array / Map / Struct)
-- ============================================================
DROP TABLE IF EXISTS parquet_complex_types_test;
CREATE TABLE parquet_complex_types_test (
  id          BIGINT,
  name        STRING,
  tags        ARRAY<STRING>,
  props       MAP<STRING, STRING>,
  address     STRUCT<city:STRING, street:STRING, zip_code:STRING>,
  orders      ARRAY<STRUCT<order_id:BIGINT, amount:DECIMAL(18,2), item_count:INT>>
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 插入复杂类型数据 (CTS / CTAS)
INSERT INTO parquet_complex_types_test
SELECT
  1 AS id,
  'user_1' AS name,
  ARRAY('tag_a', 'tag_b', 'tag_c') AS tags,
  MAP('key1', 'val1', 'key2', 'val2') AS props,
  NAMED_STRUCT('city', 'Beijing', 'street', 'Chang\'an Ave', 'zip_code', '100000') AS address,
  ARRAY(
    NAMED_STRUCT('order_id', CAST(1001 AS BIGINT), 'amount', CAST(299.99 AS DECIMAL(18,2)), 'item_count', 3),
    NAMED_STRUCT('order_id', CAST(1002 AS BIGINT), 'amount', CAST(159.50 AS DECIMAL(18,2)), 'item_count', 1)
  ) AS orders
UNION ALL
SELECT
  2 AS id,
  'user_2' AS name,
  ARRAY('tag_d') AS tags,
  MAP('key3', 'val3') AS props,
  NAMED_STRUCT('city', 'Shanghai', 'street', 'Nanjing Rd', 'zip_code', '200000') AS address,
  ARRAY(
    NAMED_STRUCT('order_id', CAST(2001 AS BIGINT), 'amount', CAST(899.00 AS DECIMAL(18,2)), 'item_count', 5)
  ) AS orders;

-- ============================================================
-- 3.7.2 复杂类型查询
-- ============================================================
-- Array 展开
SELECT id, name, tag
FROM parquet_complex_types_test
LATERAL VIEW EXPLODE(tags) t AS tag;

-- Map 访问
SELECT id, name, props['key1'] AS prop_key1
FROM parquet_complex_types_test;

-- Struct 字段访问
SELECT id, name, address.city, address.street
FROM parquet_complex_types_test;

-- 嵌套 Struct Array 展开
SELECT id, name, o.order_id, o.amount, o.item_count
FROM parquet_complex_types_test
LATERAL VIEW EXPLODE(orders) t AS o;
```

### 3.8 Schema 演化测试

```sql
-- ============================================================
-- 3.8.1 列增加
-- ============================================================
ALTER TABLE parquet_snappy_test ADD COLUMNS (
  new_col_string  STRING  COMMENT '新增字符串列',
  new_col_int     INT     COMMENT '新增整数列'
);

-- 写入新列数据
INSERT INTO parquet_snappy_test
SELECT *, CAST('new_value' AS STRING), CAST(999 AS INT)
FROM test_data_10k
LIMIT 100;

-- 验证 Schema 变更后旧数据可读
SELECT id, name, new_col_string, new_col_int
FROM parquet_snappy_test
WHERE id <= 10;
-- 预期: 旧数据的 new_col 字段为 NULL

-- ============================================================
-- 3.8.2 列类型变更 (Hive3 部分支持, Hive4 支持更全)
-- ============================================================
-- INT -> BIGINT (安全升级)
ALTER TABLE parquet_snappy_test CHANGE COLUMN new_col_int new_col_int BIGINT;

-- ============================================================
-- 3.8.3 列重命名
-- ============================================================
ALTER TABLE parquet_snappy_test CHANGE COLUMN new_col_string renamed_col STRING;

-- ============================================================
-- 3.8.4 Spark 端 Schema Merge (代码方式)
-- ============================================================
/*
// spark-shell:
val oldSchema = spark.table("parquet_snappy_test").schema
val newData = spark.range(1, 100)
  .withColumn("name", lit("test"))
  .withColumn("extra_field", lit("extra"))   // 新增字段

newData.write
  .mode("append")
  .option("mergeSchema", "true")
  .insertInto("parquet_snappy_test")
*/
```

---

## 4. ORC 格式测试

### 4.1 测试矩阵

| 测试项     | 压缩编码 | 说明                      |
| ---------- | -------- | ------------------------- |
| 读写       | none     | 无压缩                    |
| 读写       | snappy   | 快速压缩                  |
| 读写       | zlib     | ORC 默认                  |
| 读写       | lz4      | 高速压缩                  |
| 读写       | zstd     | 新一代高压缩比            |
| 读写       | lzo      | 需 native lib             |
| 分区读写   | zlib     | 分区表 + ORC              |
| ACID事务   | zlib     | Hive ACID 全功能测试      |
| 谓词下推   | zlib     | ORC 索引 (bloom filter)   |
| Stripe优化 | zlib     | stripe size / row index   |

### 4.2 建表语句

```sql
-- ============================================================
-- 4.2.1 ORC + ZLIB (默认)
-- ============================================================
USE spark4_test_db;

DROP TABLE IF EXISTS orc_zlib_test;
CREATE TABLE orc_zlib_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
STORED AS ORC
TBLPROPERTIES (
  'orc.compress'      = 'ZLIB',
  'orc.stripe.size'   = '67108864',     -- 64MB
  'orc.row.index.stride' = '10000'
);

-- ============================================================
-- 4.2.2 ORC + SNAPPY
-- ============================================================
DROP TABLE IF EXISTS orc_snappy_test;
CREATE TABLE orc_snappy_test LIKE orc_zlib_test;
ALTER TABLE orc_snappy_test SET TBLPROPERTIES (
  'orc.compress' = 'SNAPPY'
);

-- ============================================================
-- 4.2.3 ORC + ZSTD
-- ============================================================
DROP TABLE IF EXISTS orc_zstd_test;
CREATE TABLE orc_zstd_test LIKE orc_zlib_test;
ALTER TABLE orc_zstd_test SET TBLPROPERTIES (
  'orc.compress' = 'ZSTD'
);

-- ============================================================
-- 4.2.4 ORC + LZ4
-- ============================================================
DROP TABLE IF EXISTS orc_lz4_test;
CREATE TABLE orc_lz4_test LIKE orc_zlib_test;
ALTER TABLE orc_lz4_test SET TBLPROPERTIES (
  'orc.compress' = 'LZ4'
);

-- ============================================================
-- 4.2.5 ORC + LZO
-- ============================================================
DROP TABLE IF EXISTS orc_lzo_test;
CREATE TABLE orc_lzo_test LIKE orc_zlib_test;
ALTER TABLE orc_lzo_test SET TBLPROPERTIES (
  'orc.compress' = 'LZO'
);

-- ============================================================
-- 4.2.6 ORC + 无压缩
-- ============================================================
DROP TABLE IF EXISTS orc_none_test;
CREATE TABLE orc_none_test LIKE orc_zlib_test;
ALTER TABLE orc_none_test SET TBLPROPERTIES (
  'orc.compress' = 'NONE'
);

-- ============================================================
-- 4.2.7 ORC + Bloom Filter 索引
-- ============================================================
DROP TABLE IF EXISTS orc_bloom_test;
CREATE TABLE orc_bloom_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
STORED AS ORC
TBLPROPERTIES (
  'orc.compress'          = 'ZLIB',
  'orc.bloom.filter.columns' = 'id,category',
  'orc.bloom.filter.fpp'  = '0.05',
  'orc.stripe.size'       = '67108864'
);
```

### 4.3 写入测试 (INSERT)

```sql
-- ============================================================
-- 4.3.1 INSERT INTO (所有 ORC 表)
-- ============================================================
INSERT INTO orc_zlib_test   SELECT * FROM test_data_10k;
INSERT INTO orc_snappy_test SELECT * FROM test_data_10k;
INSERT INTO orc_zstd_test   SELECT * FROM test_data_10k;
INSERT INTO orc_lz4_test    SELECT * FROM test_data_10k;
INSERT INTO orc_lzo_test    SELECT * FROM test_data_10k;
INSERT INTO orc_none_test   SELECT * FROM test_data_10k;
INSERT INTO orc_bloom_test  SELECT * FROM test_data_10k;

-- ============================================================
-- 4.3.2 INSERT OVERWRITE
-- ============================================================
INSERT OVERWRITE TABLE orc_zlib_test
SELECT * FROM test_data_10k
WHERE id <= 5000;

-- ============================================================
-- 4.3.3 Spark DataFrame 写入 ORC
-- ============================================================
/*
// spark-shell:
spark.table("test_data_10k")
  .write
  .mode("append")
  .format("orc")
  .option("compression", "zstd")
  .insertInto("orc_zstd_test")
*/
```

### 4.4 读取与谓词下推测试

```sql
-- ============================================================
-- 4.4.1 基础查询
-- ============================================================
SELECT 'orc_zlib' AS tbl, count(*) FROM orc_zlib_test
UNION ALL
SELECT 'orc_snappy', count(*) FROM orc_snappy_test
UNION ALL
SELECT 'orc_zstd', count(*) FROM orc_zstd_test
UNION ALL
SELECT 'orc_lz4', count(*) FROM orc_lz4_test
UNION ALL
SELECT 'orc_lzo', count(*) FROM orc_lzo_test
UNION ALL
SELECT 'orc_none', count(*) FROM orc_none_test
UNION ALL
SELECT 'orc_bloom', count(*) FROM orc_bloom_test;

-- ============================================================
-- 4.4.2 谓词下推验证
-- ============================================================
EXPLAIN EXTENDED
SELECT * FROM orc_zlib_test
WHERE id = 5000;
-- 预期: ORC 利用 stripe 级别的 row index 跳过不匹配 stripe

-- ============================================================
-- 4.4.3 Bloom Filter 有效性验证
-- ============================================================
EXPLAIN EXTENDED
SELECT * FROM orc_bloom_test
WHERE id = 9999 AND category = 'category_5';
-- 预期: ORC 利用 bloom filter 快速排除不匹配文件

-- ============================================================
-- 4.4.4 聚合查询
-- ============================================================
SELECT
  category,
  count(*)          AS cnt,
  sum(amount)       AS total_amount,
  avg(score)        AS avg_score,
  min(biz_date)     AS first_date,
  max(biz_date)     AS last_date
FROM orc_zstd_test
GROUP BY category
ORDER BY cnt DESC;

-- ============================================================
-- 4.4.5 窗口函数
-- ============================================================
SELECT
  id,
  name,
  amount,
  ROW_NUMBER()    OVER (PARTITION BY category ORDER BY amount DESC) AS rank_by_amount,
  SUM(amount)     OVER (PARTITION BY category)                      AS category_total,
  AVG(score)      OVER (PARTITION BY category)                      AS category_avg_score
FROM orc_zstd_test
WHERE category IN ('category_1', 'category_2', 'category_3');
```

### 4.5 ACID 事务表测试 (ORC 默认支持)

```sql
-- ============================================================
-- 4.5.1 Hive ACID 全功能表 (ORC, 支持 UPDATE/DELETE)
-- ============================================================
DROP TABLE IF EXISTS orc_full_acid_test;
CREATE TABLE orc_full_acid_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING
)
CLUSTERED BY (id) INTO 5 BUCKETS
STORED AS ORC
TBLPROPERTIES (
  'transactional'       = 'true',
  'transactional_properties' = 'default',  -- Hive3 default = full ACID
  'orc.compress'        = 'ZLIB'
);

INSERT INTO orc_full_acid_test SELECT
  id, name, amount, create_time, biz_date, is_active, score, category
FROM test_data_10k;

-- ============================================================
-- 4.5.2 UPDATE 操作
-- ============================================================
UPDATE orc_full_acid_test
SET name    = 'updated_bulk',
    amount  = amount * 1.1,
    score   = score + 10
WHERE category = 'category_5';

-- 验证更新
SELECT id, name, amount, score
FROM orc_full_acid_test
WHERE category = 'category_5'
LIMIT 10;

-- ============================================================
-- 4.5.3 DELETE 操作
-- ============================================================
DELETE FROM orc_full_acid_test
WHERE biz_date < '2024-03-01';

-- 验证删除
SELECT count(*) FROM orc_full_acid_test;

-- ============================================================
-- 4.5.4 MERGE 操作 (完整 Upsert)
-- ============================================================
-- 创建增量数据表
DROP TABLE IF EXISTS orc_delta_data;
CREATE TABLE orc_delta_data (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING,
  op_type         STRING     -- 'I'=Insert, 'U'=Update, 'D'=Delete
)
STORED AS ORC;

INSERT INTO orc_delta_data VALUES
  (99991, 'new_user_1', 500.00, CAST('2024-06-01 10:00:00' AS TIMESTAMP),
   CAST('2024-06-01' AS DATE), true, 85, 'category_1', 'I'),
  (99992, 'new_user_2', 600.00, CAST('2024-06-02 10:00:00' AS TIMESTAMP),
   CAST('2024-06-02' AS DATE), false, 90, 'category_2', 'I'),
  (1, 'updated_user_1', 999.99, CAST('2024-06-01 10:00:00' AS TIMESTAMP),
   CAST('2024-06-01' AS DATE), true, 100, 'category_1', 'U'),
  (2, 'updated_user_2', 888.88, CAST('2024-06-02 10:00:00' AS TIMESTAMP),
   CAST('2024-06-02' AS DATE), true, 95, 'category_2', 'U');

-- MERGE: Upsert
MERGE INTO orc_full_acid_test AS target
USING (
  SELECT * FROM orc_delta_data WHERE op_type IN ('I', 'U')
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
  name        = source.name,
  amount      = source.amount,
  score       = source.score,
  is_active   = source.is_active,
  category    = source.category
WHEN NOT MATCHED THEN INSERT VALUES (
  source.id, source.name, source.amount, source.create_time,
  source.biz_date, source.is_active, source.score, source.category
);

-- 验证 MERGE 结果
SELECT count(*) AS total_after_merge FROM orc_full_acid_test;
SELECT * FROM orc_full_acid_test WHERE id IN (1, 2, 99991, 99992);

-- ============================================================
-- 4.5.5 时间旅行查询 (Hive3+ Iceberg / Hive4)
-- 仅对 Iceberg 或启用了版本管理的表有效
-- ============================================================
-- 查看表快照 (需 Iceberg 格式)
-- SELECT * FROM orc_iceberg_test VERSION AS OF <snapshot_id>;

-- Hive ACID: 查看 compaction 状态
SHOW COMPACTIONS;
```

### 4.6 ORC 分区表测试

```sql
-- ============================================================
-- 4.6.1 ORC 分区表 + Bucket 分桶
-- ============================================================
DROP TABLE IF EXISTS orc_partitioned_bucketed_test;
CREATE TABLE orc_partitioned_bucketed_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  is_active       BOOLEAN,
  score           BIGINT,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
PARTITIONED BY (biz_date DATE, category STRING)
CLUSTERED BY (id) SORTED BY (id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES (
  'orc.compress'       = 'ZLIB',
  'orc.bloom.filter.columns' = 'id'
);

-- 动态分区 + 分桶写入
SET hive.enforce.bucketing = true;
SET hive.enforce.sorting   = true;

INSERT INTO orc_partitioned_bucketed_test
  PARTITION (biz_date, category)
SELECT id, name, amount, create_time, is_active, score,
       metric_double, metric_float, biz_date, category
FROM test_data_10k;

-- Bucket Pruning 验证
EXPLAIN EXTENDED
SELECT * FROM orc_partitioned_bucketed_test
WHERE id = 5000;
-- 预期: 只扫描 1 个 bucket
```

---

## 5. 内部表 (Managed Table) 测试

### 5.1 测试场景总览

| 场景               | 格式    | 操作                          |
| ------------------ | ------- | ----------------------------- |
| 创建-插入-删除     | Parquet | CREATE/INSERT/DROP            |
| 创建-插入-删除     | ORC     | CREATE/INSERT/DROP            |
| 生命周期           | -       | DROP 时数据是否被删除         |
| 重命名             | -       | ALTER RENAME                  |
| 截断               | -       | TRUNCATE                      |
| 元数据操作         | -       | DESCRIBE/SHOW CREATE TABLE    |

### 5.2 测试 SQL

```sql
-- ============================================================
-- 5.2.1 创建内部表 (Parquet)
-- ============================================================
USE spark4_test_db;

DROP TABLE IF EXISTS managed_parquet_test;
CREATE TABLE managed_parquet_test (
  id          BIGINT,
  name        STRING,
  amount      DECIMAL(18,2),
  create_time TIMESTAMP,
  biz_date    DATE
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 注入数据
INSERT INTO managed_parquet_test
SELECT id, name, amount, create_time, biz_date
FROM test_data_10k;

-- ============================================================
-- 5.2.2 创建内部表 (ORC)
-- ============================================================
DROP TABLE IF EXISTS managed_orc_test;
CREATE TABLE managed_orc_test (
  id          BIGINT,
  name        STRING,
  amount      DECIMAL(18,2),
  create_time TIMESTAMP,
  biz_date    DATE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'ZLIB');

INSERT INTO managed_orc_test
SELECT id, name, amount, create_time, biz_date
FROM test_data_10k;

-- ============================================================
-- 5.2.3 验证表类型
-- ============================================================
DESCRIBE FORMATTED managed_parquet_test;
-- 检查输出中 Table Type 是否为 MANAGED_TABLE

DESCRIBE FORMATTED managed_orc_test;
-- 检查输出中 Table Type 是否为 MANAGED_TABLE

-- ============================================================
-- 5.2.4 查看表在 HDFS 的位置
-- ============================================================
DESCRIBE EXTENDED managed_parquet_test;
-- Location: hdfs://namenode:8020/warehouse/spark4_test_db.db/managed_parquet_test

-- ============================================================
-- 5.2.5 TRUNCATE 测试
-- ============================================================
-- 记下当前行数
SELECT count(*) AS before_truncate FROM managed_parquet_test;

-- 截断表
TRUNCATE TABLE managed_parquet_test;

-- 验证截断后为 0
SELECT count(*) AS after_truncate FROM managed_parquet_test;
-- 预期: 0

-- 重新插入
INSERT INTO managed_parquet_test
SELECT id, name, amount, create_time, biz_date
FROM test_data_10k
WHERE id <= 100;

SELECT count(*) AS after_reinsert FROM managed_parquet_test;
-- 预期: 100

-- ============================================================
-- 5.2.6 表重命名
-- ============================================================
ALTER TABLE managed_parquet_test RENAME TO managed_parquet_renamed_test;

-- 验证新表名可用
SELECT count(*) FROM managed_parquet_renamed_test;

-- 恢复表名
ALTER TABLE managed_parquet_renamed_test RENAME TO managed_parquet_test;

-- ============================================================
-- 5.2.7 DROP 内部表 (数据也被删除)
-- ============================================================
DROP TABLE IF EXISTS managed_parquet_test;

-- 尝试用 spark.sql 直接读原来的路径 (应失败，因为数据已被删除)
-- 在 Spark Shell 中:
/*
val path = "/warehouse/spark4_test_db.db/managed_parquet_test"
val df = spark.read.parquet(path)
// 预期: AnalysisException - Path does not exist
*/

-- ============================================================
-- 5.2.8 Spark DataFrame 方式管理内部表
-- ============================================================
/*
// spark-shell:
// 创建内部表
spark.table("test_data_10k")
  .write
  .mode("overwrite")
  .format("parquet")
  .saveAsTable("managed_from_spark_test")

// 验证
spark.sql("SELECT count(*) FROM managed_from_spark_test").show()

// 追加写入
spark.table("test_data_10k")
  .write
  .mode("append")
  .insertInto("managed_from_spark_test")

// 更新表属性
spark.sql("""
  ALTER TABLE managed_from_spark_test
  SET TBLPROPERTIES ('parquet.compression' = 'ZSTD')
""")

// 删除表
spark.sql("DROP TABLE managed_from_spark_test")
*/
```

---

## 6. 外部表 (External Table) 测试

### 6.1 测试场景总览

| 场景             | 操作                                      |
| ---------------- | ----------------------------------------- |
| 创建-读取        | CREATE EXTERNAL TABLE + LOCATION          |
| DROP 后数据保留  | 验证 DROP 只删除元数据不删数据            |
| 路径挂载         | 已有 Parquet/ORC 路径直接创建外表         |
| 多表共享数据     | 两外表指向同一路径                        |
| 分区发现         | MSCK REPAIR / ALTER ADD PARTITION         |
| 自定义路径       | LOCATION 指向非 warehouse 目录            |

### 6.2 测试 SQL

```sql
-- ============================================================
-- 6.2.1 先通过 Spark 写入数据 (生成外部路径)
-- ============================================================
-- Spark SQL 模式: 直接用 INSERT 到指定路径
-- （注意: 直接用 spark.sql 写外部路径需要权限）

-- 方法: 先在 HDFS 创建目录并写入 Parquet
/*
// spark-shell:
val dataPath = "/data/external/parquet_raw_data"

spark.table("test_data_10k")
  .write
  .mode("overwrite")
  .format("parquet")
  .option("compression", "snappy")
  .save(dataPath)
*/

-- 或使用 CTAS 指定外部位置 (Spark SQL 语法)
CREATE TABLE spark4_test_db.parquet_raw_external_data
USING PARQUET
OPTIONS (compression 'snappy')
LOCATION '/data/external/parquet_raw_data'
AS SELECT * FROM test_data_10k;

-- ============================================================
-- 6.2.2 创建 Hive 外部表指向已有数据
-- ============================================================
DROP TABLE IF EXISTS external_parquet_test;
CREATE EXTERNAL TABLE external_parquet_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
STORED AS PARQUET
LOCATION '/data/external/parquet_raw_data';

-- 验证可直接查询
SELECT count(*) FROM external_parquet_test;
-- 预期: 10000

-- ============================================================
-- 6.2.3 外部表类型验证
-- ============================================================
DESCRIBE FORMATTED external_parquet_test;
-- 检查 Table Type: EXTERNAL_TABLE

-- ============================================================
-- 6.2.4 DROP 外部表 - 数据不应删除
-- ============================================================
DROP TABLE IF EXISTS external_parquet_test;

-- 验证数据仍在
/*
// spark-shell:
val df = spark.read.parquet("/data/external/parquet_raw_data")
println(s"Row count after DROP: ${df.count()}")
// 预期: 10000 (数据仍然存在)
*/

-- 重新创建外部表，数据立即可用
CREATE EXTERNAL TABLE external_parquet_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
STORED AS PARQUET
LOCATION '/data/external/parquet_raw_data';

SELECT count(*) FROM external_parquet_test;
-- 预期: 10000

-- ============================================================
-- 6.2.5 多表共享同一外部路径
-- ============================================================
CREATE EXTERNAL TABLE external_parquet_test_v2
LIKE external_parquet_test
LOCATION '/data/external/parquet_raw_data';

-- 两张外表应返回相同数据
SELECT 'v1' AS version, count(*) AS cnt FROM external_parquet_test
UNION ALL
SELECT 'v2', count(*) FROM external_parquet_test_v2;

DROP TABLE IF EXISTS external_parquet_test_v2;

-- ============================================================
-- 6.2.6 外部分区表
-- ============================================================

-- 步骤 1: 创建分区外表
DROP TABLE IF EXISTS external_partitioned_test;
CREATE EXTERNAL TABLE external_partitioned_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  is_active       BOOLEAN,
  score           BIGINT,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
PARTITIONED BY (biz_date DATE, category STRING)
STORED AS PARQUET
LOCATION '/data/external/partitioned_data';

-- 步骤 2: 写入分区数据 (Spark 方式生成分区目录)
/*
// spark-shell:
spark.table("test_data_10k")
  .write
  .mode("overwrite")
  .partitionBy("biz_date", "category")
  .format("parquet")
  .save("/data/external/partitioned_data")
*/

-- 步骤 3: 修复分区元数据
MSCK REPAIR TABLE external_partitioned_test;

-- 或手动添加分区
ALTER TABLE external_partitioned_test
  ADD PARTITION (biz_date = '2024-06-15', category = 'category_3')
  LOCATION '/data/external/partitioned_data/biz_date=2024-06-15/category=category_3';

-- 步骤 4: 验证分区数据
SHOW PARTITIONS external_partitioned_test;
SELECT count(*) FROM external_partitioned_test;

-- ============================================================
-- 6.2.7 外部表 Schema 变化后数据兼容性
-- ============================================================
-- 先写入一些数据到独立的 ORC 外部路径
/*
// spark-shell:
spark.table("test_data_10k")
  .select("id", "name", "amount", "create_time")
  .write.mode("overwrite").format("orc").save("/data/external/orc_partial")
*/

-- 创建外部表 (列数少于数据实际列数时，新增列补 NULL)
CREATE EXTERNAL TABLE external_orc_partial_test (
  id          BIGINT,
  name        STRING,
  amount      DECIMAL(18,2),
  create_time TIMESTAMP,
  biz_date    DATE,        -- 数据中无此列，读取时为 NULL
  is_active   BOOLEAN      -- 数据中无此列，读取时为 NULL
)
STORED AS ORC
LOCATION '/data/external/orc_partial';

SELECT id, name, amount, create_time, biz_date, is_active
FROM external_orc_partial_test
LIMIT 10;
-- 预期: biz_date 和 is_active 均为 NULL，但不报错

DROP TABLE IF EXISTS external_orc_partial_test;

-- ============================================================
-- 6.2.8 外部 ORC 表 + ACID 测试
-- ============================================================
DROP TABLE IF EXISTS external_orc_acid_test;
CREATE EXTERNAL TABLE external_orc_acid_test (
  id          BIGINT,
  name        STRING,
  amount      DECIMAL(18,2),
  biz_date    DATE,
  category    STRING
)
STORED AS ORC
LOCATION '/data/external/orc_acid_data'
TBLPROPERTIES (
  'transactional'       = 'true',
  'transactional_properties' = 'insert_only'
);

INSERT INTO external_orc_acid_test
SELECT id, name, amount, biz_date, category
FROM test_data_10k
WHERE id <= 100;

-- UPDATE / DELETE 对 insert_only 外部事务表的限制验证
-- Hive3: insert_only 表仅支持 INSERT, 不支持 UPDATE/DELETE
-- Hive4: 对 full ACID 外部表支持更完整

SELECT count(*) FROM external_orc_acid_test;
DROP TABLE IF EXISTS external_orc_acid_test;
```

---

## 7. Iceberg 表测试

### 7.1 Iceberg 核心特性测试矩阵

| 特性               | 说明                                         |
| ------------------ | -------------------------------------------- |
| Schema Evolution   | 列增加/删除/重命名/类型变更/列重排序         |
| Partition Evolution| 分区方案变更不重写数据                       |
| Time Travel        | 历史快照查询 / 回滚                          |
| ACID 事务          | UPDATE / DELETE / MERGE                      |
| Hidden Partitioning| 隐藏分区 (不暴露物理分区列)                  |
| 文件格式           | Parquet / ORC / Avro                         |
| 压缩编码           | Snappy / ZSTD / Gzip / LZ4                   |
| 表版本升级         | v1 → v2 升级                                 |
| 分支/标签          | Branch / Tag 管理 (V2)                       |

### 7.2 Iceberg 配置与 Catalog 设置

```sql
-- ============================================================
-- 7.2.1 使用 Hive Catalog (MetaStore 管理模式)
-- ============================================================

-- 如果已通过 spark-defaults.conf 配置，直接使用:
USE iceberg_catalog;
CREATE NAMESPACE IF NOT EXISTS iceberg_test_db;
USE iceberg_catalog.iceberg_test_db;

-- 或者通过 Spark SQL 动态设置:
/*
SET spark.sql.catalog.iceberg_catalog = org.apache.iceberg.spark.SparkCatalog;
SET spark.sql.catalog.iceberg_catalog.type = hive;
SET spark.sql.catalog.iceberg_catalog.uri = thrift://hive-metastore-host:9083;
*/

-- ============================================================
-- 7.2.2 验证 Iceberg 环境
-- ============================================================
SHOW NAMESPACES IN iceberg_catalog;
-- 应看到 iceberg_test_db
```

### 7.3 Iceberg 基础表操作

```sql
-- ============================================================
-- 7.3.1 创建 Iceberg 表 (Parquet + Snappy)
-- ============================================================
USE iceberg_catalog.iceberg_test_db;

DROP TABLE IF EXISTS iceberg_basic_test;
CREATE TABLE iceberg_basic_test (
  id              BIGINT        COMMENT '主键',
  name            STRING        COMMENT '名称',
  amount          DECIMAL(18,2) COMMENT '金额',
  create_time     TIMESTAMP     COMMENT '创建时间',
  biz_date        DATE          COMMENT '业务日期',
  is_active       BOOLEAN       COMMENT '是否活跃',
  score           BIGINT        COMMENT '评分',
  category        STRING        COMMENT '分类',
  metric_double   DOUBLE        COMMENT '指标',
  metric_float    FLOAT         COMMENT '指标'
)
USING ICEBERG
PARTITIONED BY (category)           -- Iceberg 隐藏分区
TBLPROPERTIES (
  'write.format.default'     = 'parquet',
  'write.parquet.compression-codec' = 'snappy',
  'format-version'           = '2',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max'       = '10'
);

-- ============================================================
-- 7.3.2 创建 Iceberg 表 (Parquet + ZSTD)
-- ============================================================
DROP TABLE IF EXISTS iceberg_zstd_test;
CREATE TABLE iceberg_zstd_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING
)
USING ICEBERG
TBLPROPERTIES (
  'write.format.default'     = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.compression-level' = '3',
  'format-version'           = '2'
);

-- ============================================================
-- 7.3.3 创建 Iceberg 表 (ORC + ZLIB)
-- ============================================================
DROP TABLE IF EXISTS iceberg_orc_test;
CREATE TABLE iceberg_orc_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING
)
USING ICEBERG
TBLPROPERTIES (
  'write.format.default'     = 'orc',
  'write.orc.compression-codec' = 'zlib',
  'format-version'           = '2'
);
```

### 7.4 Iceberg 写入与查询

```sql
-- ============================================================
-- 7.4.1 INSERT INTO
-- ============================================================
USE iceberg_catalog.iceberg_test_db;

INSERT INTO iceberg_basic_test
SELECT * FROM spark_catalog.spark4_test_db.test_data_10k;

INSERT INTO iceberg_zstd_test
SELECT id, name, amount, create_time, biz_date, is_active, score, category
FROM spark_catalog.spark4_test_db.test_data_10k;

INSERT INTO iceberg_orc_test
SELECT id, name, amount, create_time, biz_date, is_active, score, category
FROM spark_catalog.spark4_test_db.test_data_10k;

-- ============================================================
-- 7.4.2 INSERT OVERWRITE
-- ============================================================
INSERT OVERWRITE iceberg_basic_test
SELECT * FROM spark_catalog.spark4_test_db.test_data_10k
WHERE id <= 5000;

-- ============================================================
-- 7.4.3 聚合查询
-- ============================================================
SELECT
  category,
  count(*)          AS cnt,
  sum(amount)       AS total_amount,
  avg(score)        AS avg_score
FROM iceberg_basic_test
GROUP BY category
ORDER BY cnt DESC;

-- ============================================================
-- 7.4.4 Spark DataFrame 读写 Iceberg
-- ============================================================
/*
// spark-shell:
// 读取
val df = spark.table("iceberg_catalog.iceberg_test_db.iceberg_basic_test")
df.groupBy("category").count().show()

// 追加写入
spark.table("spark_catalog.spark4_test_db.test_data_10k")
  .write
  .format("iceberg")
  .mode("append")
  .save("iceberg_catalog.iceberg_test_db.iceberg_basic_test")

// CTAS
spark.sql("""
  CREATE OR REPLACE TABLE iceberg_catalog.iceberg_test_db.iceberg_ctas_test
  USING ICEBERG
  AS SELECT * FROM spark_catalog.spark4_test_db.test_data_10k
""")
*/
```

### 7.5 Iceberg UPDATE / DELETE / MERGE

```sql
-- ============================================================
-- 7.5.1 UPDATE (行级更新)
-- ============================================================
USE iceberg_catalog.iceberg_test_db;

-- 单条件更新
UPDATE iceberg_basic_test
SET name    = 'updated_by_iceberg',
    amount  = amount * 1.2,
    score   = score + 5
WHERE category = 'category_5';

-- 验证
SELECT id, name, amount, score
FROM iceberg_basic_test
WHERE category = 'category_5'
LIMIT 10;

-- ============================================================
-- 7.5.2 DELETE (行级删除)
-- ============================================================
DELETE FROM iceberg_basic_test
WHERE biz_date < '2024-03-01';

SELECT count(*) AS after_delete FROM iceberg_basic_test;

-- ============================================================
-- 7.5.3 MERGE (Upsert)
-- ============================================================
-- 准备增量数据
CREATE OR REPLACE TEMP VIEW iceberg_delta AS
SELECT
  1 AS id, CAST('merged_user_1' AS STRING) AS name,
  CAST(999.99 AS DECIMAL(18,2)) AS amount, CAST('2024-12-01 10:00:00' AS TIMESTAMP) AS create_time,
  CAST('2024-12-01' AS DATE) AS biz_date, true AS is_active,
  CAST(100 AS BIGINT) AS score, CAST('category_new' AS STRING) AS category,
  CAST(99.9 AS DOUBLE) AS metric_double, CAST(99.9 AS FLOAT) AS metric_float
UNION ALL
SELECT
  99991 AS id, CAST('new_user_99' AS STRING) AS name,
  CAST(500.00 AS DECIMAL(18,2)) AS amount, CAST('2024-12-01 10:00:00' AS TIMESTAMP) AS create_time,
  CAST('2024-12-01' AS DATE) AS biz_date, true AS is_active,
  CAST(85 AS BIGINT) AS score, CAST('category_new' AS STRING) AS category,
  CAST(50.0 AS DOUBLE) AS metric_double, CAST(50.0 AS FLOAT) AS metric_float;

-- 执行 MERGE
MERGE INTO iceberg_basic_test AS target
USING iceberg_delta AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
  name        = source.name,
  amount      = source.amount,
  score       = source.score,
  category    = source.category
WHEN NOT MATCHED THEN INSERT *;

-- 验证 MERGE
SELECT id, name, amount, score, category
FROM iceberg_basic_test
WHERE id IN (1, 99991);
```

### 7.6 Iceberg Schema Evolution

```sql
-- ============================================================
-- 7.6.1 添加列
-- ============================================================
USE iceberg_catalog.iceberg_test_db;

ALTER TABLE iceberg_basic_test ADD COLUMNS (
  new_col_1       STRING  COMMENT '新增列1',
  new_col_2       INT     COMMENT '新增列2',
  nested_struct   STRUCT<a:INT, b:STRING> COMMENT '嵌套结构体'
);

-- 验证 Schema
DESCRIBE iceberg_basic_test;

-- 插入含新列的数据
INSERT INTO iceberg_basic_test
SELECT
  id + 100000, name, amount, create_time, biz_date, is_active, score, category,
  metric_double, metric_float,
  'new_data', 999, NAMED_STRUCT('a', 1, 'b', 'test')
FROM spark_catalog.spark4_test_db.test_data_10k
WHERE id <= 10;

-- 验证新旧数据共存
SELECT id, name, new_col_1, new_col_2, nested_struct
FROM iceberg_basic_test
WHERE id > 100000
LIMIT 5;
-- 预期: 新数据有 new_col 值

SELECT id, name, new_col_1, new_col_2
FROM iceberg_basic_test
WHERE id <= 10;
-- 预期: 旧数据 new_col 为 NULL

-- ============================================================
-- 7.6.2 列重命名 (Iceberg V2)
-- ============================================================
ALTER TABLE iceberg_basic_test RENAME COLUMN new_col_1 TO renamed_col_1;

-- ============================================================
-- 7.6.3 列类型提升 (安全升级)
-- ============================================================
ALTER TABLE iceberg_basic_test ALTER COLUMN new_col_2 TYPE BIGINT;

-- ============================================================
-- 7.6.4 列重排序
-- ============================================================
ALTER TABLE iceberg_basic_test ALTER COLUMN renamed_col_1 AFTER name;

-- ============================================================
-- 7.6.5 删除列 (Iceberg V2)
-- ============================================================
ALTER TABLE iceberg_basic_test DROP COLUMN nested_struct;

-- ============================================================
-- 7.6.6 查看 Schema 历史
-- ============================================================
SELECT * FROM iceberg_catalog.iceberg_test_db.iceberg_basic_test.history;
SELECT * FROM iceberg_catalog.iceberg_test_db.iceberg_basic_test.snapshots;
```

### 7.7 Iceberg Partition Evolution

```sql
-- ============================================================
-- 7.7.1 查看当前分区方案
-- ============================================================
SELECT * FROM iceberg_catalog.iceberg_test_db.iceberg_basic_test.partitions;

-- ============================================================
-- 7.7.2 变更分区方案 (不重写数据!)
-- ============================================================
-- 从按月分区改为按天分区 (Iceberg 支持多级分区演进)
ALTER TABLE iceberg_basic_test
SET PARTITION SPEC (month(biz_date));

-- 或者添加时间分区 (从仅 category 改为 category + month(biz_date))
-- ALTER TABLE iceberg_basic_test
-- ADD PARTITION FIELD month(biz_date);

-- ============================================================
-- 7.7.3 验证分区变更不影响已有数据
-- ============================================================
SELECT count(*) FROM iceberg_basic_test;
-- 预期: 所有数据仍然可读

-- 新写入数据将按新分区方案组织
INSERT INTO iceberg_basic_test
SELECT * FROM spark_catalog.spark4_test_db.test_data_10k
WHERE id <= 100;
```

### 7.8 Iceberg Time Travel

```sql
-- ============================================================
-- 7.8.1 查看快照列表
-- ============================================================
USE iceberg_catalog.iceberg_test_db;

SELECT
  snapshot_id,
  parent_snapshot_id,
  committed_at,
  operation,
  summary
FROM iceberg_basic_test.snapshots
ORDER BY committed_at DESC;

-- ============================================================
-- 7.8.2 按快照ID查询历史数据
-- ============================================================
-- 假设 snapshot_id 为 1234567890123456789
SELECT count(*) AS historical_count
FROM iceberg_basic_test
VERSION AS OF 1234567890123456789;

-- ============================================================
-- 7.8.3 按时间戳查询历史数据
-- ============================================================
SELECT count(*) AS time_travel_count
FROM iceberg_basic_test
TIMESTAMP AS OF '2026-07-01 10:00:00';

-- ============================================================
-- 7.8.4 回滚表到指定快照
-- ============================================================
-- 先记下当前数据量
SELECT count(*) AS before_rollback FROM iceberg_basic_test;

-- 回滚到 DELETE 操作之前的快照
-- CALL iceberg_catalog.system.rollback_to_snapshot(
--   'iceberg_test_db.iceberg_basic_test',
--   1234567890123456789
-- );

-- 验证回滚后数据恢复
-- SELECT count(*) AS after_rollback FROM iceberg_basic_test;
-- 预期: 恢复到回滚前的行数

-- ============================================================
-- 7.8.5 设置当前快照 (Spark 3.3+ 语法)
-- ============================================================
-- CALL iceberg_catalog.system.set_current_snapshot(
--   'iceberg_test_db.iceberg_basic_test',
--   1234567890123456789
-- );
```

### 7.9 Iceberg 表维护

```sql
-- ============================================================
-- 7.9.1 查看表元数据文件
-- ============================================================
SELECT * FROM iceberg_basic_test.files;
SELECT * FROM iceberg_basic_test.manifests;

-- ============================================================
-- 7.9.2 清理历史快照 (保留最近5个)
-- ============================================================
-- CALL iceberg_catalog.system.expire_snapshots(
--   'iceberg_test_db.iceberg_basic_test',
--   TIMESTAMP '2026-06-01 00:00:00',
--   5   -- 最少保留的快照数
-- );

-- ============================================================
-- 7.9.3 清理孤立文件
-- ============================================================
-- CALL iceberg_catalog.system.remove_orphan_files(
--   'iceberg_test_db.iceberg_basic_test'
-- );

-- ============================================================
-- 7.9.4 重写数据文件 (合并小文件)
-- ============================================================
-- CALL iceberg_catalog.system.rewrite_data_files(
--   'iceberg_test_db.iceberg_basic_test'
-- );

-- ============================================================
-- 7.9.5 重写清单文件
-- ============================================================
-- CALL iceberg_catalog.system.rewrite_manifests(
--   'iceberg_test_db.iceberg_basic_test'
-- );
```

---

## 8. Paimon 表测试

### 8.1 Paimon 核心特性测试矩阵

| 特性               | 说明                                      |
| ------------------ | ----------------------------------------- |
| Primary Key Table  | 主键表 (支持 Upsert/Delete)               |
| Append-Only Table  | 追加表 (日志/时序场景)                    |
| Changelog          | 变更日志 (CDC 支持)                       |
| Schema Evolution   | 列变更/重命名/类型升级                    |
| Partition          | 分区表 + 分区演进                         |
| Time Travel        | 快照查询与回滚                            |
| File Format        | Parquet / ORC                             |
| Compression        | Snappy / ZSTD / LZ4 / Gzip                |
| Bucket             | 分桶策略 (fixed/dynamic)                  |
| Merge Engine       | deduplicate / partial-update / aggregation|
| Tag                | 标签管理                                  |
| Consumer ID        | 流式消费位点                              |

### 8.2 Paimon 配置与 Catalog 设置

```sql
-- ============================================================
-- 8.2.1 使用 Paimon Catalog
-- ============================================================
USE paimon_catalog;
CREATE DATABASE IF NOT EXISTS paimon_test_db;
USE paimon_catalog.paimon_test_db;

-- ============================================================
-- 8.2.2 验证 Paimon 环境
-- ============================================================
SHOW DATABASES IN paimon_catalog;
-- 应看到 paimon_test_db
```

### 8.3 Paimon 主键表 (Primary Key Table)

```sql
-- ============================================================
-- 8.3.1 创建主键表 (Parquet + ZSTD)
-- ============================================================
USE paimon_catalog.paimon_test_db;

DROP TABLE IF EXISTS paimon_pk_parquet_test;
CREATE TABLE paimon_pk_parquet_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING,
  metric_double   DOUBLE,
  metric_float    FLOAT
)
TBLPROPERTIES (
  'primary-key'               = 'id',
  'bucket'                    = '8',
  'file.format'               = 'parquet',
  'file.compression'          = 'zstd',
  'file.compression.zstd-level' = '3',
  'snapshot.num-retained.min' = '3',
  'snapshot.num-retained.max' = '10',
  'changelog-producer'        = 'input',
  'merge-engine'              = 'deduplicate'
);

-- ============================================================
-- 8.3.2 创建主键表 (ORC + ZLIB)
-- ============================================================
DROP TABLE IF EXISTS paimon_pk_orc_test;
CREATE TABLE paimon_pk_orc_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  biz_date        DATE,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING
)
TBLPROPERTIES (
  'primary-key'       = 'id',
  'bucket'            = '4',
  'file.format'       = 'orc',
  'file.compression'  = 'zlib',
  'merge-engine'      = 'deduplicate'
);

-- ============================================================
-- 8.3.3 创建复合主键表
-- ============================================================
DROP TABLE IF EXISTS paimon_composite_pk_test;
CREATE TABLE paimon_composite_pk_test (
  user_id         BIGINT,
  order_id        BIGINT,
  product_name    STRING,
  amount          DECIMAL(18,2),
  order_date      DATE,
  status          STRING
)
TBLPROPERTIES (
  'primary-key'       = 'user_id,order_id',
  'bucket'            = '8',
  'file.format'       = 'parquet',
  'file.compression'  = 'snappy',
  'merge-engine'      = 'deduplicate'
);
```

### 8.4 Paimon 追加表 (Append-Only Table)

```sql
-- ============================================================
-- 8.4.1 创建追加表 (日志/事件场景)
-- ============================================================
USE paimon_catalog.paimon_test_db;

DROP TABLE IF EXISTS paimon_append_test;
CREATE TABLE paimon_append_test (
  event_id        BIGINT,
  event_type      STRING,
  event_data      STRING,
  event_time      TIMESTAMP,
  user_id         BIGINT
)
TBLPROPERTIES (
  'bucket'                  = '4',
  'file.format'             = 'parquet',
  'file.compression'        = 'zstd',
  'write-mode'              = 'append-only',
  'snapshot.num-retained.min' = '5'
);

-- ============================================================
-- 8.4.2 创建分区追加表
-- ============================================================
DROP TABLE IF EXISTS paimon_partitioned_append_test;
CREATE TABLE paimon_partitioned_append_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  create_time     TIMESTAMP,
  is_active       BOOLEAN,
  score           BIGINT,
  category        STRING
)
PARTITIONED BY (biz_date, category)
TBLPROPERTIES (
  'bucket'                  = '8',
  'file.format'             = 'parquet',
  'file.compression'        = 'snappy',
  'write-mode'              = 'append-only'
);
```

### 8.5 Paimon 写入与查询

```sql
-- ============================================================
-- 8.5.1 INSERT INTO (主键表 - 首次写入)
-- ============================================================
USE paimon_catalog.paimon_test_db;

INSERT INTO paimon_pk_parquet_test
SELECT * FROM spark_catalog.spark4_test_db.test_data_10k;

INSERT INTO paimon_pk_orc_test
SELECT id, name, amount, create_time, biz_date, is_active, score, category
FROM spark_catalog.spark4_test_db.test_data_10k;

INSERT INTO paimon_composite_pk_test
SELECT
  cast(rand(id * 37) * 1000 AS BIGINT)    AS user_id,
  id                                        AS order_id,
  concat('product_', cast(cast(rand(id * 41) * 100 AS INT) AS STRING)) AS product_name,
  amount,
  biz_date,
  CASE WHEN rand(id * 43) > 0.7 THEN 'completed'
       WHEN rand(id * 43) > 0.4 THEN 'processing'
       ELSE 'pending'
  END                                       AS status
FROM spark_catalog.spark4_test_db.test_data_10k;

-- ============================================================
-- 8.5.2 INSERT INTO (追加表)
-- ============================================================
INSERT INTO paimon_append_test
SELECT
  id,
  concat('event_type_', cast(cast(rand(id * 53) * 5 AS INT) AS STRING)),
  concat('{"data": "event_data_', cast(id AS STRING), '"}'),
  create_time,
  cast(rand(id * 37) * 1000 AS BIGINT)
FROM spark_catalog.spark4_test_db.test_data_10k;

INSERT INTO paimon_partitioned_append_test
  PARTITION (biz_date, category)
SELECT id, name, amount, create_time, is_active, score, biz_date, category
FROM spark_catalog.spark4_test_db.test_data_10k;

-- ============================================================
-- 8.5.3 INSERT OVERWRITE (Paimon 支持)
-- ============================================================
-- 对于非主键表
INSERT OVERWRITE paimon_append_test
SELECT * FROM paimon_append_test WHERE event_id <= 5000;

-- 对于主键表 (动态覆写分区)
-- INSERT OVERWRITE paimon_pk_parquet_test
-- SELECT * FROM spark_catalog.spark4_test_db.test_data_10k WHERE id <= 1000;

-- ============================================================
-- 8.5.4 查询验证
-- ============================================================
SELECT 'pk_parquet' AS tbl, count(*) AS cnt FROM paimon_pk_parquet_test
UNION ALL
SELECT 'pk_orc', count(*) FROM paimon_pk_orc_test
UNION ALL
SELECT 'composite_pk', count(*) FROM paimon_composite_pk_test
UNION ALL
SELECT 'append', count(*) FROM paimon_append_test;

-- 聚合查询
SELECT
  category,
  count(*)          AS cnt,
  sum(amount)       AS total_amount,
  avg(score)        AS avg_score
FROM paimon_pk_parquet_test
GROUP BY category
ORDER BY cnt DESC;

-- ============================================================
-- 8.5.5 Spark DataFrame 写入 Paimon
-- ============================================================
/*
// spark-shell:
spark.table("spark_catalog.spark4_test_db.test_data_10k")
  .write
  .format("paimon")
  .mode("append")
  .save("paimon_catalog.paimon_test_db.paimon_pk_parquet_test")

// 或使用 SQL CTAS
spark.sql("""
  CREATE TABLE paimon_catalog.paimon_test_db.paimon_ctas_test
  TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '4',
    'file.format' = 'parquet'
  )
  AS SELECT * FROM spark_catalog.spark4_test_db.test_data_10k
""")
*/
```

### 8.6 Paimon 主键表 Upsert

```sql
-- ============================================================
-- 8.6.1 INSERT INTO 重复主键 → 自动 Upsert (deduplicate引擎)
-- ============================================================
USE paimon_catalog.paimon_test_db;

-- 插入包含已存在 ID 的数据 (模拟更新)
INSERT INTO paimon_pk_parquet_test
SELECT
  id,                                             -- 已存在的 id
  concat('updated_', name)    AS name,            -- 更新名称
  amount * 1.5                AS amount,           -- 更新金额
  create_time,
  biz_date,
  NOT is_active               AS is_active,        -- 翻转状态
  score + 20                  AS score,            -- 增加评分
  concat('new_', category)    AS category,         -- 更新分类
  metric_double * 2.0,
  metric_float * 2.0
FROM spark_catalog.spark4_test_db.test_data_10k
WHERE id <= 100;

-- 验证 - 应该只有最新的值
SELECT id, name, amount, is_active, score, category
FROM paimon_pk_parquet_test
WHERE id <= 10
ORDER BY id;
-- 预期: 显示更新后的值 (updated_name, 1.5x amount, flipped is_active, score+20)

-- 验证总数 - 不因重复插入而翻倍
SELECT count(*) FROM paimon_pk_parquet_test;
-- 预期: 10000 (与首次插入一致)

-- ============================================================
-- 8.6.2 复合主键 Upsert
-- ============================================================
INSERT INTO paimon_composite_pk_test VALUES
  (1, 1001, 'updated_product', 999.99, CAST('2024-12-01' AS DATE), 'completed');

SELECT * FROM paimon_composite_pk_test
WHERE user_id = 1 AND order_id = 1001;
-- 预期: 显示更新后的数据

-- ============================================================
-- 8.6.3 使用 partial-update 引擎 (部分字段更新)
-- ============================================================
DROP TABLE IF EXISTS paimon_partial_update_test;
CREATE TABLE paimon_partial_update_test (
  id              BIGINT,
  name            STRING,
  amount          DECIMAL(18,2),
  score           BIGINT,
  category        STRING,
  update_count    INT
)
TBLPROPERTIES (
  'primary-key'    = 'id',
  'bucket'         = '4',
  'file.format'    = 'parquet',
  'merge-engine'   = 'partial-update',
  'fields.name.ignore-retract' = 'true'
);

-- 首次插入完整数据
INSERT INTO paimon_partial_update_test
SELECT
  id, name, amount, score, category, 1 AS update_count
FROM spark_catalog.spark4_test_db.test_data_10k
WHERE id <= 100;

-- 部分更新 (只更新 name, 其他字段保持不变)
INSERT INTO paimon_partial_update_test
SELECT
  id,
  concat('partial_updated_', name) AS name,
  NULL AS amount,              -- NULL 不会覆盖原值 (需配合 ignore-retract)
  NULL AS score,
  NULL AS category,
  2 AS update_count
FROM spark_catalog.spark4_test_db.test_data_10k
WHERE id <= 50;

-- 验证 partial update 效果
SELECT id, name, amount, score, category, update_count
FROM paimon_partial_update_test
WHERE id <= 10
ORDER BY id;
```

### 8.7 Paimon DELETE

```sql
-- ============================================================
-- 8.7.1 DELETE (主键表) - Paimon 0.9+ 支持
-- ============================================================
USE paimon_catalog.paimon_test_db;

-- 单条件删除
DELETE FROM paimon_pk_parquet_test
WHERE biz_date < '2024-03-01';

SELECT count(*) AS after_delete FROM paimon_pk_parquet_test;

-- 多条件删除
DELETE FROM paimon_pk_parquet_test
WHERE category IN ('category_8', 'category_9')
  AND score < 50;

SELECT count(*) AS after_multi_delete FROM paimon_pk_parquet_test;

-- ============================================================
-- 8.7.2 追加表删除 (Paimon append-only 表 DELETE 限制)
-- ============================================================
-- Paimon 追加表不直接支持 DELETE
-- 变通方案: 使用 INSERT OVERWRITE 过滤
INSERT OVERWRITE paimon_append_test
SELECT * FROM paimon_append_test
WHERE event_id > 1000;
```

### 8.8 Paimon Schema Evolution

```sql
-- ============================================================
-- 8.8.1 添加列
-- ============================================================
USE paimon_catalog.paimon_test_db;

ALTER TABLE paimon_pk_parquet_test ADD COLUMNS (
  new_field_string  STRING  COMMENT '新增字符串字段',
  new_field_decimal DECIMAL(10, 2) COMMENT '新增decimal字段'
);

-- 写入新列数据
INSERT INTO paimon_pk_parquet_test
SELECT
  888888 AS id, 'test_new_col' AS name, CAST(100.00 AS DECIMAL(18,2)) AS amount,
  CAST('2026-07-01 00:00:00' AS TIMESTAMP) AS create_time, CAST('2026-07-01' AS DATE) AS biz_date,
  true AS is_active, CAST(50 AS BIGINT) AS score, 'test_cat' AS category,
  CAST(1.0 AS DOUBLE) AS metric_double, CAST(1.0 AS FLOAT) AS metric_float,
  'new_value' AS new_field_string, CAST(999.99 AS DECIMAL(10,2)) AS new_field_decimal;

-- 验证
SELECT id, name, new_field_string, new_field_decimal
FROM paimon_pk_parquet_test
WHERE id = 888888;

-- ============================================================
-- 8.8.2 列重命名
-- ============================================================
ALTER TABLE paimon_pk_parquet_test RENAME COLUMN new_field_string TO renamed_string_field;

-- ============================================================
-- 8.8.3 修改列注释
-- ============================================================
ALTER TABLE paimon_pk_parquet_test ALTER COLUMN renamed_string_field COMMENT '重命名后的字符串字段';

-- ============================================================
-- 8.8.4 查看 Schema 变更历史
-- ============================================================
DESCRIBE HISTORY paimon_pk_parquet_test;
-- 或 (取决于版本)
-- SELECT * FROM paimon_pk_parquet_test$schemas;
```

### 8.9 Paimon Time Travel

```sql
-- ============================================================
-- 8.9.1 查看快照
-- ============================================================
USE paimon_catalog.paimon_test_db;

SELECT * FROM paimon_pk_parquet_test$snapshots;
-- 记下 snapshot_id

-- ============================================================
-- 8.9.2 按快照ID查询
-- ============================================================
-- Paimon 时间旅行语法:
SELECT count(*) FROM paimon_pk_parquet_test
/*+ OPTIONS('scan.snapshot-id' = '1') */;

-- 或通过 SET 方式:
-- SET paimon.scan.snapshot-id = 1;
-- SELECT count(*) FROM paimon_pk_parquet_test;

-- ============================================================
-- 8.9.3 按时间戳查询
-- ============================================================
SELECT count(*) FROM paimon_pk_parquet_test
/*+ OPTIONS('scan.timestamp-millis' = '1688000000000') */;

-- ============================================================
-- 8.9.4 创建 Tag (标签)
-- ============================================================
-- CALL paimon_catalog.sys.create_tag(
--   'paimon_test_db.paimon_pk_parquet_test',
--   'release_v1',
--   1   -- snapshot_id
-- );

-- 按 Tag 查询
-- SELECT * FROM paimon_pk_parquet_test
-- /*+ OPTIONS('scan.tag-name' = 'release_v1') */;
```

### 8.10 Paimon 表维护

```sql
-- ============================================================
-- 8.10.1 查看表文件
-- ============================================================
SELECT * FROM paimon_pk_parquet_test$files;

-- ============================================================
-- 8.10.2 合并小文件 (Compaction)
-- ============================================================
-- CALL paimon_catalog.sys.compact('paimon_test_db.paimon_pk_parquet_test');

-- ============================================================
-- 8.10.3 清理过期快照
-- ============================================================
-- CALL paimon_catalog.sys.expire_snapshots(
--   'paimon_test_db.paimon_pk_parquet_test',
--   5   -- 保留最近5个快照
-- );

-- ============================================================
-- 8.10.4 清理孤立文件
-- ============================================================
-- CALL paimon_catalog.sys.remove_orphan_files('paimon_test_db.paimon_pk_parquet_test');
```

---

## 9. 混合场景与边界测试

### 9.1 跨格式互操作

```sql
-- ============================================================
-- 9.1.1 Parquet 表数据迁移到 ORC 表
-- ============================================================
USE spark_catalog.spark4_test_db;

-- 源 (Parquet) → 目标 (ORC)
INSERT INTO orc_zstd_test
SELECT * FROM parquet_zstd_test;

-- 验证数据一致性
SELECT
  (SELECT count(*) FROM parquet_zstd_test) AS parquet_count,
  (SELECT count(*) FROM orc_zstd_test)     AS orc_count;

-- ============================================================
-- 9.1.2 Hive 表数据迁移到 Iceberg
-- ============================================================
INSERT INTO iceberg_catalog.iceberg_test_db.iceberg_basic_test
SELECT * FROM spark_catalog.spark4_test_db.parquet_zstd_test;

-- 验证
SELECT
  (SELECT count(*) FROM spark_catalog.spark4_test_db.parquet_zstd_test) AS hive_count,
  (SELECT count(*) FROM iceberg_catalog.iceberg_test_db.iceberg_basic_test) AS iceberg_count;

-- ============================================================
-- 9.1.3 Hive 表数据迁移到 Paimon
-- ============================================================
INSERT INTO paimon_catalog.paimon_test_db.paimon_pk_parquet_test
SELECT * FROM spark_catalog.spark4_test_db.parquet_snappy_test;

SELECT count(*) AS paimon_count
FROM paimon_catalog.paimon_test_db.paimon_pk_parquet_test;

-- ============================================================
-- 9.1.4 跨 Catalog JOIN
-- ============================================================
SELECT
  h.category,
  count(*)          AS hive_cnt,
  SUM(i.amount)     AS iceberg_total
FROM spark_catalog.spark4_test_db.parquet_zstd_test h
JOIN iceberg_catalog.iceberg_test_db.iceberg_basic_test i
  ON h.id = i.id
WHERE h.category IN ('category_1', 'category_2', 'category_3')
GROUP BY h.category;

-- ============================================================
-- 9.1.5 联合查询 (跨格式 UNION ALL)
-- ============================================================
SELECT 'parquet_snappy' AS source, id, name, amount, biz_date
FROM spark_catalog.spark4_test_db.parquet_snappy_test
WHERE id <= 100
UNION ALL
SELECT 'orc_zlib', id, name, amount, biz_date
FROM spark_catalog.spark4_test_db.orc_zlib_test
WHERE id <= 100
UNION ALL
SELECT 'iceberg', id, name, amount, biz_date
FROM iceberg_catalog.iceberg_test_db.iceberg_basic_test
WHERE id <= 100;
```

### 9.2 并发与事务隔离

```sql
-- ============================================================
-- 9.2.1 同一 Iceberg 表的并发写入 (乐观并发控制)
-- ============================================================
-- 会话1:
-- INSERT INTO iceberg_catalog.iceberg_test_db.iceberg_concurrent_test
-- SELECT * FROM test_data_10k WHERE id <= 5000;

-- 会话2 (同时执行):
-- INSERT INTO iceberg_catalog.iceberg_test_db.iceberg_concurrent_test
-- SELECT * FROM test_data_10k WHERE id > 5000;

-- 预期: Iceberg 的乐观锁保证两个写入都成功 (无冲突时)
-- 验证: SELECT count(*) 应为 10000

-- ============================================================
-- 9.2.2 并发 UPDATE (冲突检测)
-- ============================================================
-- 会话1:
-- UPDATE iceberg_basic_test SET score = score + 1 WHERE id = 1;

-- 会话2 (同时):
-- UPDATE iceberg_basic_test SET score = score + 2 WHERE id = 1;

-- 预期: 先提交者成功，后提交者可能重试或失败
--       (取决于 isolation-level 配置)
```

### 9.3 边界情况测试

```sql
-- ============================================================
-- 9.3.1 空表操作
-- ============================================================
USE spark_catalog.spark4_test_db;

DROP TABLE IF EXISTS empty_table_test;
CREATE TABLE empty_table_test (
  id    BIGINT,
  name  STRING
)
STORED AS PARQUET;

-- 空表查询
SELECT count(*) FROM empty_table_test;
-- 预期: 0

-- 空表 INSERT
INSERT INTO empty_table_test VALUES (1, 'first_row');
SELECT count(*) FROM empty_table_test;
-- 预期: 1

-- 空表 DELETE (Hive ACID)
DELETE FROM empty_table_test WHERE 1=1;
SELECT count(*) FROM empty_table_test;
-- 预期: 0

DROP TABLE IF EXISTS empty_table_test;

-- ============================================================
-- 9.3.2 NULL 值处理
-- ============================================================
DROP TABLE IF EXISTS null_handling_test;
CREATE TABLE null_handling_test (
  id              BIGINT,
  nullable_str    STRING,
  nullable_int    INT,
  nullable_double DOUBLE
)
STORED AS PARQUET;

INSERT INTO null_handling_test VALUES
  (1, 'value', 100, 1.5),
  (2, NULL, NULL, NULL),
  (3, '', 0, 0.0),
  (4, '  ', -1, -1.0),
  (5, 'NULL_STRING', NULL, NULL);

SELECT * FROM null_handling_test;
SELECT count(*) FROM null_handling_test WHERE nullable_str IS NULL;
SELECT count(*) FROM null_handling_test WHERE nullable_int IS NULL;
SELECT count(*) FROM null_handling_test WHERE nullable_str = '';
SELECT id, nullable_str FROM null_handling_test WHERE nullable_str IS NOT NULL;

DROP TABLE IF EXISTS null_handling_test;

-- ============================================================
-- 9.3.3 特殊字符/多语言 (UTF-8)
-- ============================================================
DROP TABLE IF EXISTS unicode_test;
CREATE TABLE unicode_test (
  id    BIGINT,
  text  STRING
)
STORED AS PARQUET;

INSERT INTO unicode_test VALUES
  (1, '你好，世界！'),
  (2, 'こんにちは世界'),
  (3, '안녕하세요 세계'),
  (4, 'Привет, мир!'),
  (5, '😀🎉💻'),
  (6, 'Emoji: 👨‍💻👩‍💻'),
  (7, 'Mixed: Hello 你好 こんにちは');

SELECT * FROM unicode_test;
SELECT * FROM unicode_test WHERE text LIKE '%你好%';

DROP TABLE IF EXISTS unicode_test;

-- ============================================================
-- 9.3.4 极大行/极大列测试
-- ============================================================
-- 大字符串
DROP TABLE IF EXISTS large_value_test;
CREATE TABLE large_value_test (
  id          BIGINT,
  large_text  STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'ZSTD');

-- 插入大文本 (>1MB)
INSERT INTO large_value_test
SELECT
  1,
  repeat('ABCDEFGHIJ', 100000);  -- 1MB 字符串

-- 验证数据完整性
SELECT id, length(large_text) AS text_length
FROM large_value_test;
-- 预期: text_length = 1000000

DROP TABLE IF EXISTS large_value_test;

-- ============================================================
-- 9.3.5 分区边界值
-- ============================================================
DROP TABLE IF EXISTS partition_boundary_test;
CREATE TABLE partition_boundary_test (
  id          BIGINT,
  name        STRING,
  amount      DECIMAL(18,2)
)
PARTITIONED BY (biz_date STRING)
STORED AS PARQUET;

-- 特殊分区值
INSERT INTO partition_boundary_test PARTITION (biz_date = '2024-02-29') VALUES (1, 'leap_day', 100.00);
INSERT INTO partition_boundary_test PARTITION (biz_date = '9999-12-31') VALUES (2, 'max_date', 200.00);
INSERT INTO partition_boundary_test PARTITION (biz_date = '0001-01-01') VALUES (3, 'min_date', 300.00);
INSERT INTO partition_boundary_test PARTITION (biz_date = 'has=equals') VALUES (4, 'special_char', 400.00);
INSERT INTO partition_boundary_test PARTITION (biz_date = 'has/slash') VALUES (5, 'slash_val', 500.00);

SHOW PARTITIONS partition_boundary_test;

SELECT * FROM partition_boundary_test
WHERE biz_date = 'has=equals';

DROP TABLE IF EXISTS partition_boundary_test;
```

### 9.4 性能相关测试

```sql
-- ============================================================
-- 9.4.1 大范围聚合 (配合 AQE)
-- ============================================================
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;

SELECT
  category,
  biz_date,
  count(*)          AS cnt,
  sum(amount)       AS total_amount,
  avg(score)        AS avg_score,
  min(create_time)  AS first_time,
  max(create_time)  AS last_time
FROM parquet_partitioned_test
GROUP BY category, biz_date
ORDER BY category, biz_date;

-- ============================================================
-- 9.4.2 分区表全表扫描 vs 分区裁剪性能对比
-- ============================================================
-- 全表扫描 (基线)
SELECT count(*), sum(amount)
FROM parquet_partitioned_test;

-- 分区裁剪 (单分区)
SELECT count(*), sum(amount)
FROM parquet_partitioned_test
WHERE biz_date = '2024-06-15' AND category = 'category_3';
-- 预期: 执行时间显著少于全表扫描

-- ============================================================
-- 9.4.3 Bucket Join 优化验证
-- ============================================================
SET spark.sql.autoBroadcastJoinThreshold = -1;  -- 禁用 Broadcast Join
SET spark.sql.bucketing.enabled = true;

-- 如果两个表使用相同分桶策略，应走 Bucket Join
/*
EXPLAIN EXTENDED
SELECT /*+ BROADCAST(t2) * / a.*, b.name AS b_name
FROM orc_partitioned_bucketed_test a
JOIN orc_partitioned_bucketed_test b ON a.id = b.id
LIMIT 100;
*/
```

### 9.5 综合测试脚本 (一键执行)

```sql
-- ============================================================
-- 9.5.1 全量功能回归测试 SQL 脚本
-- 可按需顺序执行，每段独立验证
-- ============================================================

-- Phase 1: 环境检查
SELECT '=== Phase 1: Environment Check ===' AS phase;
SHOW DATABASES;
SHOW CATALOGS;
SELECT version() AS spark_version;
SET spark.sql.hive.metastore.version;

-- Phase 2: Parquet 全压缩编码
SELECT '=== Phase 2: Parquet Compression Tests ===' AS phase;
SOURCE parquet_write_test.sql;       -- 各压缩编码 INSERT
SOURCE parquet_read_test.sql;        -- 各表 SELECT + EXPLAIN

-- Phase 3: ORC 全压缩编码 + ACID
SELECT '=== Phase 3: ORC Compression + ACID Tests ===' AS phase;
SOURCE orc_write_test.sql;
SOURCE orc_acid_test.sql;            -- UPDATE/DELETE/MERGE

-- Phase 4: 内表/外表生命周期
SELECT '=== Phase 4: Managed/External Table Lifecycle ===' AS phase;
SOURCE managed_table_test.sql;
SOURCE external_table_test.sql;

-- Phase 5: Iceberg 全套
SELECT '=== Phase 5: Iceberg Full Tests ===' AS phase;
SOURCE iceberg_basic_test.sql;
SOURCE iceberg_schema_evolution_test.sql;
SOURCE iceberg_time_travel_test.sql;

-- Phase 6: Paimon 全套
SELECT '=== Phase 6: Paimon Full Tests ===' AS phase;
SOURCE paimon_pk_test.sql;
SOURCE paimon_schema_evolution_test.sql;
SOURCE paimon_time_travel_test.sql;

-- Phase 7: 混合场景
SELECT '=== Phase 7: Mixed Scenarios ===' AS phase;
SOURCE cross_format_test.sql;
SOURCE boundary_test.sql;
```

---

## 10. 性能基准参考

### 10.1 压缩编码对比 (参考值)

| 格式      | 压缩编码    | 相对大小 | 写入速度 | 读取速度 | 推荐场景     |
| ------- | ------- | ---- | ---- | ---- | -------- |
| Parquet | none    | 100% | 最快   | 最快   | 临时/中间数据  |
| Parquet | snappy  | ~30% | 快    | 快    | 默认通用     |
| Parquet | lz4     | ~32% | 很快   | 很快   | 高速读写     |
| Parquet | zstd(3) | ~22% | 中等   | 快    | 存储优化推荐   |
| Parquet | zstd(9) | ~18% | 较慢   | 中等   | 归档存储     |
| Parquet | gzip    | ~20% | 慢    | 中等   | 最大化压缩    |
| Parquet | brotli  | ~18% | 很慢   | 快    | Web/跨域传输 |
| ORC     | none    | 100% | 最快   | 最快   | 临时数据     |
| ORC     | zlib    | ~20% | 慢    | 中等   | 默认通用     |
| ORC     | snappy  | ~28% | 快    | 快    | 查询优先     |
| ORC     | lz4     | ~30% | 很快   | 很快   | 高速ETL    |
| ORC     | zstd    | ~20% | 中等   | 快    | 新一代推荐    |

> 注: 以上为 10M 行混合数据(数值+文本)的参考值，实际结果取决于数据特征。

### 10.2 表格式特性对比

| 特性              | Hive Managed | Hive External | Iceberg    | Paimon     |
| ----------------- | ------------ | ------------- | ---------- | ---------- |
| Schema Evolution  | 部分支持     | 部分支持      | ✓ 完整支持 | ✓ 完整支持 |
| Partition Evolution| ✗           | ✗             | ✓          | ✓          |
| Time Travel       | ✗            | ✗             | ✓          | ✓          |
| ACID UPDATE/DELETE| Hive3+部分   | 受限          | ✓          | ✓          |
| MERGE/Upsert      | Hive3+部分   | ✗             | ✓          | ✓ (主键表) |
| 流式写入          | ✗            | ✗             | ✓          | ✓          |
| 数据 Compaction   | Hive ACID    | ✗             | ✓          | ✓          |
| 多引擎兼容        | Spark/Hive   | 所有          | 多引擎     | 多引擎     |
| DROP 数据行为     | 删除数据     | 保留数据      | 可配置     | 可配置     |
| 并发写入控制      | 悲观锁       | 无            | 乐观锁     | 乐观锁     |

---

## 附录 A: 常用诊断命令

```sql
-- ============================================================
-- A.1 表信息查询
-- ============================================================

-- 查看表的 HDFS 路径和文件
DESCRIBE FORMATTED <table_name>;
DESCRIBE EXTENDED <table_name>;
SHOW CREATE TABLE <table_name>;

-- 查看表统计信息
ANALYZE TABLE <table_name> COMPUTE STATISTICS;
ANALYZE TABLE <table_name> COMPUTE STATISTICS FOR COLUMNS id, name, category;
DESCRIBE EXTENDED <table_name>;  -- 查看 statistics 部分

-- 查看文件数量和大小
-- (Spark SQL)
/*
spark.sql("DESCRIBE EXTENDED <table_name>").show(200, false)
*/

-- ============================================================
-- A.2 文件系统检查 (HDFS 命令)
-- ============================================================

-- 查看表目录结构
-- hadoop fs -ls -R /warehouse/spark4_test_db.db/<table_name>

-- 查看 Parquet 文件元信息
-- parquet-tools meta hdfs://namenode:8020/warehouse/.../part-00000-xxx.parquet
-- parquet-tools schema hdfs://namenode:8020/warehouse/.../part-00000-xxx.parquet
-- parquet-tools dump --head hdfs://namenode:8020/warehouse/.../part-00000-xxx.parquet

-- 查看 ORC 文件元信息
-- orc-tools meta hdfs://namenode:8020/warehouse/.../000000_0
-- orc-tools data hdfs://namenode:8020/warehouse/.../000000_0

-- ============================================================
-- A.3 查询优化诊断
-- ============================================================

-- 查看执行计划
EXPLAIN COST <your_query>;
EXPLAIN EXTENDED <your_query>;
EXPLAIN CODEGEN <your_query>;

-- 查看 AQE 运行时指标 (Spark UI 或)
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.logLevel = DEBUG;

-- ============================================================
-- A.4 Spark 配置调试
-- ============================================================
SET -v;  -- 查看所有 Spark 配置
SET spark.sql.extensions;
SET spark.sql.catalog.iceberg_catalog;
SET spark.sql.catalog.paimon_catalog;
```

## 附录 B: 测试结果记录模板

```markdown
| 用例编号 | 测试项                     | Hive3 结果 | Hive4 结果 | 备注                      |
| -------- | -------------------------- | ---------- | ---------- | ------------------------- |
| P-S-01   | Parquet Snappy 写入        | ✓ PASS     | ✓ PASS     |                           |
| P-S-02   | Parquet Snappy 读取        | ✓ PASS     | ✓ PASS     |                           |
| P-Z-01   | Parquet ZSTD 写入          |            |            |                           |
| P-Z-02   | Parquet ZSTD 读取          |            |            |                           |
| P-G-01   | Parquet GZIP 写入          |            |            |                           |
| O-Z-01   | ORC ZLIB 写入              |            |            |                           |
| O-A-01   | ORC ACID UPDATE            |            |            | Hive3 full ACID, Hive4+  |
| I-S-01   | Iceberg Schema Add Column  |            |            | Hive4 原生支持最佳        |
| I-T-01   | Iceberg Time Travel        |            |            |                           |
| M-PK-01  | Paimon PK Upsert           |            |            |                           |
| M-D-01   | Paimon DELETE              |            |            |                           |
| CR-01    | 跨Catalog JOIN (Hive+Iceberg) |         |            |                           |
| B-01     | NULL值处理                 |            |            |                           |
| B-02     | Unicode/Emoji              |            |            |                           |
```

---

## 附录 C: 依赖与 JAR 包清单

### C.1 Maven 依赖 (pom.xml)

```xml
<!-- Iceberg -->
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-spark-runtime-3.5_2.13</artifactId>
    <version>1.5.2</version>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-hive-runtime</artifactId>
    <version>1.5.2</version>
</dependency>

<!-- Paimon -->
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-spark-3.5</artifactId>
    <version>0.9.0</version>
</dependency>
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-spark-common</artifactId>
    <version>0.9.0</version>
</dependency>

<!-- Hive Metastore Client -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-metastore</artifactId>
    <version>3.1.3</version> <!-- 或 4.0.0 -->
</dependency>

<!-- 压缩编解码器 -->
<dependency>
    <groupId>com.github.luben</groupId>
    <artifactId>zstd-jni</artifactId>
    <version>1.5.6-3</version>
</dependency>
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.0</version>
</dependency>
<dependency>
    <groupId>com.hadoop.compression</groupId>
    <artifactId>hadoop-lzo</artifactId>
    <version>0.4.20</version>
</dependency>
```

### C.2 spark-submit 启动示例

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "spark4-hive-integration-test" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.2,\
org.apache.paimon:paimon-spark-3.5:0.9.0,\
com.github.luben:zstd-jni:1.5.6-3 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg_catalog.type=hive \
  --conf spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore-host:9083 \
  --conf spark.sql.catalog.paimon_catalog=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon_catalog.warehouse=/warehouse/paimon \
  --jars /opt/spark/jars/hive-metastore-3.1.3.jar \
  your-test-application.jar
```

---

> **文档维护说明**: 请根据实际测试环境调整 Metastore URI、HDFS 路径等配置。每个测试用例执行后请在附录 B 的模板中记录结果。测试过程中如遇错误，请一并记录错误信息与解决方案。

---

## 附录 D: 版本兼容性风险分析与重点测试建议

> **目标版本组合**: Spark 4.1.2 + Hive 3.1.3 / Hive 4.2.0 + Iceberg 1.6 + Paimon 0.8.2

### D.1 风险总览矩阵

```
┌──────────────────────────────────────────────────────────────────┐
│  组件交叉                   风险等级    预期结果                  │
├──────────────────────────────────────────────────────────────────┤
│  Paimon 0.8.2 + Spark 4.1.2  🔴 CRITICAL  大概率不可用           │
│  Iceberg 1.6 + Hive 4.2.0    🟡 MEDIUM    ClassLoader 冲突       │
│  Hive 3.1.3 + Spark 4.1.2    🟡 MEDIUM    部分功能退化           │
│  Hive 4.2.0 + Spark 4.1.2    🟡 MEDIUM    Metastore API 不匹配   │
│  Iceberg 1.6 + Spark 4.1.2   🟢 LOW       官方支持, 基本可用     │
│  Hive 3.1.3 ACID + Spark 4   🟡 MEDIUM    ACID 读取不完整        │
└──────────────────────────────────────────────────────────────────┘
```

---

### D.2 🔴 Paimon 0.8.2 + Spark 4.1.2 — CRITICAL

#### 根本原因

**Paimon 0.8.2 没有 `paimon-spark-4.0` 模块。**

| Paimon 版本 | 可用 Spark 模块          | Spark 4 支持 |
| ----------- | ------------------------ | ------------ |
| 0.8.2       | paimon-spark-3.2, 3.3, 3.5 | ❌ 无官方 Spark 4 JAR |
| 0.9.0       | 新增 paimon-spark-4.0     | ✅ 正式支持   |
| 1.0+        | paimon-spark-4.0, 4.1    | ✅ 完整支持   |

用户提到 `paimon-spark-common-0.8.2` 内含 `paimon-spark-3.5` 模块, 想用 `paimon-spark-3.5_2.13` 在 Spark 4 上运行。这条路**理论上可能但实践中非常危险**, 原因如下:

#### 具体不兼容点

**1. DataSource V2 API 变更 (最高风险)**

Spark 4 对 DataSource V2 接口做了大量重构。Paimon 的 Spark Catalog 和 Table 实现深度依赖这些接口:

```java
// Spark 3.5 的接口 (Paimon 0.8.2 编译时使用的)
org.apache.spark.sql.connector.catalog.TableCatalog
org.apache.spark.sql.connector.catalog.Table
org.apache.spark.sql.connector.catalog.SupportsWrite
org.apache.spark.sql.connector.write.WriteBuilder

// Spark 4.x 中这些接口的签名/包路径可能已变更
// → NoSuchMethodError / AbstractMethodError / ClassNotFoundException
```

**重点测试**: 任何涉及 Paimon Catalog 的操作——`USE paimon_catalog`、`SHOW TABLES IN paimon_catalog`、`CREATE TABLE ... USING paimon`——这些是最先暴露 API 不匹配的地方。

**2. Procedure/CALL 语法变更**

Spark 4 的 SQL 解析器从 ANTLR 3 升级到了 ANTLR 4。Paimon 如果注册了自定义 SQL 扩展（通过 `spark.sql.extensions`），解析器钩子接口发生变化:

```sql
-- Paimon 存储过程调用 (Paimon 0.8.2 基于 Spark 3.5 Parser)
CALL paimon_catalog.sys.compact('db.table');
CALL paimon_catalog.sys.expire_snapshots('db.table', 5);
```

在 Spark 4 下可能报 `ParseException` 或 `AnalysisException`。

**3. Scala 2.13 二进制兼容性**

- Paimon 0.8.2 的 `paimon-spark-3.5` 使用 Scala 2.12 编译（Spark 3.5 默认 Scala 2.12）
- Spark 4.x **只发布** Scala 2.13 版本
- Scala 2.12 和 2.13 二进制不兼容 → 即使是看似无关的类也可能因 Scala 标准库变化而崩溃

用户的环境如果确实拿到了 `paimon-spark-3.5_2.13`（Scala 2.13 编译版，Spark 3.5 确实有 2.13 变体），那 Scala 层面的风险降低，但 DataSource V2 API 问题仍然存在。

**4. 内部 Spark API 调用**

Paimon 可能调用了 Spark 内部 `private[spark]` API（如 `SQLConf`、`SparkSession` 内部字段），这些在 Spark 4 中可能已改名或被移除。

#### 必须重点测试的 Paimon 场景

| 优先级 | 测试项 | 预期失败模式 |
| ------ | ------ | ------------ |
| P0 | `USE paimon_catalog` 是否能执行 | `ClassNotFoundException: org.apache.paimon.spark.SparkCatalog` 或 `NoSuchMethodError` |
| P0 | `SHOW TABLES IN paimon_catalog` | Catalog 加载失败 |
| P0 | `CREATE TABLE ... TBLPROPERTIES ('primary-key'='id')` | DataSource V2 CreateTable 失败 |
| P0 | `INSERT INTO paimon_xxx` | WriteBuilder 接口不匹配 |
| P1 | `SELECT * FROM paimon_xxx` | ScanBuilder 接口不匹配 |
| P1 | `DELETE FROM paimon_xxx` | DataSource V2 Delete 接口变化 |
| P1 | `CALL paimon_catalog.sys.compact(...)` | SQL Parser 解析失败 |
| P1 | Paimon time travel (`$snapshots`, `$files`) | 元数据表查询语法变化 |
| P2 | `INSERT OVERWRITE` | Overwrite 接口可能变化 |
| P2 | Schema evolution (`ALTER TABLE ADD COLUMNS`) | Catalog AlterTable 接口 |

#### 最低风险的尝试方案

如果暂时无法升级 Paimon 到 0.9+，按以下顺序尝试:

```
方案 1 (推荐): 升级 Paimon → 0.9.0 或 1.0+

方案 2 (风险): 使用 paimon-spark-3.5_2.13 jar
  - spark-submit 时加 --jars paimon-spark-3.5_2.13-0.8.2.jar
  - 配置 spark.sql.catalog.paimon_catalog=org.apache.paimon.spark.SparkCatalog
  - 如果启动时直接报 ClassNotFoundException, 说明完全不可用
  - 如果启动 OK 但 CREATE TABLE 报 AbstractMethodError, 说明 DataSource V2 不兼容

方案 3 (务实): Paimon 独立部署, 通过 Hive Metastore 桥接
  - 用 Flink SQL 创建/管理 Paimon 表, 元数据注册到 Hive Metastore
  - Spark 4 通过 Hive Metastore 读到 Paimon 表的 location, 以 Hive 外表方式读取
  - 缺点: 失去 Paimon 的 upsert/time travel 等特性
```

---

### D.3 🟡 Iceberg 1.6 + Hive 4.2.0 — MEDIUM

#### 根本原因: Hive 4.2.0 内嵌了更高版本的 Iceberg

Hive 4.x 版本及其捆绑的 Iceberg 版本:

| Hive 版本 | 内嵌 Iceberg 版本 | 用户 Iceberg 版本 | 版本差 |
| --------- | ----------------- | ----------------- | ------ |
| 4.0.0-beta-1 | 1.3.0 | 1.6 | 用户版本更高 |
| 4.0.x | 1.4.3 | 1.6 | 用户版本更高 |
| 4.2.0 | **1.9.1** (推测) | **1.6** | **Hive 版本更高!** |

**核心矛盾**: Hive 4.2.0 内置了 Iceberg 1.9.x 的 JAR。当 Spark 通过 Hive Metastore 与 Hive 4.2.0 交互时，如果 HiveServer2 端加载了 Iceberg 1.9.x 的类，而 Spark 端加载了 Iceberg 1.6 的类，同一个 Iceberg 表在两端可能被以不同的 schema/API 操作，导致:

- 元数据文件格式不一致
- Hive 端写入的 snapshot 与 Spark 端的 snapshot 解析不兼容

#### 具体风险点

**1. Iceberg Catalog 不尊重 `spark.sql.hive.metastore.version`** (已确认的 Bug)

从 [apache/iceberg#10401](https://github.com/apache/iceberg/issues/10401) 可知: Iceberg 的 HiveCatalog 使用默认 ClassLoader 加载 `HiveMetaStoreClient`，而不是 Spark 的 `IsolatedClientLoader`。这意味着:

```properties
# 这个配置对 Iceberg Catalog 无效!
spark.sql.hive.metastore.version = 3.1
spark.sql.hive.metastore.jars = maven
```

**重点测试**: 在 Hive 4.2.0 的 Metastore 上使用 Iceberg 1.6 的 `SparkCatalog` (type=hive) 创建和查询表。

**2. 类路径冲突 — 同一个 JVM 中两个版本的 Iceberg**

如果 Hive 4.2.0 的 lib 目录下有 `iceberg-core-1.9.x.jar`，当 Spark 任务提交到 YARN 且 Hive 依赖被加到 classpath 时:

```
spark-submit 加载顺序:
  App classpath → Spark jars → HADOOP_CLASSPATH → HIVE_CLASSPATH
  
如果 Hive 4.2.0 的 iceberg JAR 出现在 classpath 前面
→ Iceberg 1.6 的类被 1.9.x 的类覆盖
→ 可能正常运行 (向前兼容), 也可能出现方法签名不匹配
```

**3. Hive 4 原生 Iceberg 表 vs Spark Iceberg 表**

Hive 4.2.0 支持直接使用 `CREATE TABLE ... STORED BY ICEBERG` (Hive 原生语法)，这与 Spark 的 `CREATE TABLE ... USING ICEBERG` (Spark 语法) 生成的元数据可能有差异:

```sql
-- Hive 4 原生方式 (走 Hive IcebergStorageHandler)
CREATE TABLE native_iceberg (id BIGINT, name STRING)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

-- Spark 方式 (走 SparkCatalog)
CREATE TABLE iceberg_catalog.db.spark_iceberg (id BIGINT, name STRING)
USING ICEBERG;
```

这两种方式创建的表在 Metastore 中的元数据结构不同——Hive 方式使用 `STORAGE_HANDLER`，Spark 方式使用 `iceberg` 的 catalog table 元数据。

**重点测试**:
- Spark 能否读取 Hive 4 原生创建的 Iceberg 表?
- Hive 4 能否读取 Spark 创建的 Iceberg 表?
- 如果两边同时写同一张 Iceberg 表会怎样?

#### 必须重点测试的 Iceberg 场景

| 优先级 | 测试项 | 风险说明 |
| ------ | ------ | -------- |
| P0 | Hive 4 原生 `STORED BY ICEBERG` 表能否被 Spark Iceberg Catalog 读取 | 元数据结构差异 |
| P0 | Spark Iceberg 表能否被 Hive 4 的 beeline 读取 | HiveSide Iceberg 版本匹配 |
| P1 | `CALL iceberg_catalog.system.rollback_to_snapshot(...)` 存储过程 | Procedure 参数签名变化 |
| P1 | Spark Iceberg 写入 → Hive 4 查询 (反之亦然) | 写入格式兼容性 |
| P1 | 并发写入同一张 Iceberg 表 (Spark + Hive4) | 乐观锁冲突处理差异 |
| P2 | Iceberg branch/tag 功能 | Iceberg 1.6 vs 1.9 API 差异 |
| P2 | Iceberg `expire_snapshots` / `rewrite_data_files` 维护操作 | 内部实现差异 |

#### 建议的规避方案

```
方案 A (推荐): 升级 Iceberg 到 1.9.x
  - 与 Hive 4.2.0 内嵌版本一致, 消除类冲突
  - Iceberg 1.9.x 同样支持 Spark 4.x (有 iceberg-spark-runtime-4.0)
  - 检查点: 确认 Iceberg 1.9 与 Spark 4.1.2 的兼容性矩阵

方案 B (折中): 从 Hive 4.2.0 lib 中移除内嵌 Iceberg JAR
  - 风险: Hive 4 的某些功能可能依赖这些 JAR

方案 C (隔离): Spark 和 Hive 走不同的 Metastore 实例
  - Hive 3.1.3 给 Hive 原生态, Hive 4.2.0 只做元数据存储
  - 或者反过来: Spark 走 Hive 3.1.3 Metastore, Hive 4.2.0 独立
```

---

### D.4 🟡 Hive 3.1.3 + Spark 4.1.2 — MEDIUM

#### 已知不兼容项

**1. Hive ACID 表读取限制**

Spark 4 对 Hive ACID 表的读取支持不完整:

| 操作 | Hive 3.1.3 ACID v1 (insert_only) | Hive 3.1.3 ACID v1 (full) |
| ---- | -------------------------------- | ------------------------- |
| SELECT | ✅ 支持 (Spark 4) | ⚠️ 取决于 compaction 状态 |
| INSERT | ✅ 支持 | ⚠️ 可能破坏 ACID 语义 |
| UPDATE (Spark) | ❌ Spark 不直接支持 Hive ACID UPDATE | ❌ |
| DELETE (Spark) | ❌ Spark 不直接支持 Hive ACID DELETE | ❌ |

Hive 3.1.3 的 ACID v1 使用 delta 目录结构 (`delta_xxx` / `delete_delta_xxx`)，Spark 4 的 Hive ACID reader 支持读取 compacted base 文件，但对未 compacted 的 delta 文件读取有限制。

**重点测试**:
```sql
-- 在 Hive CLI 中创建 ACID 表并写入数据
-- CREATE TABLE acid_v1_test (id BIGINT, name STRING)
--   STORED AS ORC TBLPROPERTIES ('transactional'='true');
-- INSERT INTO acid_v1_test VALUES (1, 'a'), (2, 'b');
-- UPDATE acid_v1_test SET name = 'updated' WHERE id = 1;

-- 然后在 Spark 4 中尝试读取
-- SELECT * FROM acid_v1_test;
-- 预期: 可能只读到 compaction 之前的数据, 或直接报错
```

**2. Hive UDF/SerDe 兼容性**

Spark 4 的 Hive 集成从 Spark 3.x 的 `hive-2.3.x` 升级到 `hive-3.1.x` 作为基准，但某些 Hive 3.1.3 的 SerDe 可能依赖已移除的 Hadoop API:

```sql
-- 以下 SerDe 在 Spark 4 中可能不可用:
-- LazySimpleSerDe (Hive 默认, 通常 OK)
-- OrcSerde (需确认版本匹配)
-- ParquetHiveSerDe (Hive 3.x 的, 非 Spark 原生 Parquet)
-- JsonSerDe
-- OpenCSVSerde
-- RegexSerDe
```

**3. `spark.sql.hive.convertMetastoreParquet` / `convertMetastoreOrc`**

Spark 4 默认使用自己的 Parquet/ORC reader 而非 Hive SerDe。如果 Hive 3.1.3 的表使用了特殊的 SerDe 属性，Spark 的原生 reader 可能无法正确处理:

```sql
-- 可能出问题的配置组合:
SET spark.sql.hive.convertMetastoreParquet = true;  -- Spark 4 默认
SET spark.sql.hive.convertMetastoreOrc = true;       -- Spark 4 默认

-- 如果 Hive 表用了特殊的 decimal 格式或 timestamp 格式
-- Spark 原生 reader 读出的精度可能不同
```

**重点测试**: 创建一个包含 `DECIMAL(38,18)` 和 `TIMESTAMP WITH LOCAL TIME ZONE` 类型的 Parquet/ORC 表，比较 Hive CLI 和 Spark SQL 读出的结果是否一致。

**4. Dynamic Partition Pruning 与 Hive 分区表**

Spark 4 的 DPP (Dynamic Partition Pruning) 优化在 Hive 分区表上可能失效或产生错误结果，特别是分区列类型为 `DATE`/`TIMESTAMP` 时。

**5. Table Statistics 不一致**

Hive 3.1.3 和 Spark 4 对 `ANALYZE TABLE` 生成的统计信息存储格式不同。如果 Hive 先执行了 `ANALYZE TABLE`，Spark 4 读取这些统计信息时可能得到错误的值，导致 CBO (Cost-Based Optimizer) 生成次优计划。

#### 对 Hive 3.1.3 的重点测试

| 优先级 | 测试项 | 风险说明 |
| ------ | ------ | -------- |
| P0 | Hive ACID compacted 表读取 | Delta 文件读取兼容性 |
| P0 | `spark.sql.hive.convertMetastoreOrc=true` 读取 Hive ORC 表 | 数据精度差异 |
| P0 | `spark.sql.hive.convertMetastoreParquet=true` 读取 Hive Parquet 表 | Decimal/Timestamp 精度 |
| P1 | Hive UDF 在 Spark SQL 中调用 | UDF 类加载失败 |
| P1 | `ANALYZE TABLE` 统计信息互通 | CBO 计划错误 |
| P1 | Dynamic Partition Pruning 正确性 | 结果集不完整 |
| P2 | `INSERT OVERWRITE` 动态分区 | 分区目录冲突 |
| P2 | Hive Materialized View 的 Spark 读取 | 仅 Hive 3.x 的部分 MV 支持 |

---

### D.5 🟡 Hive 4.2.0 + Spark 4.1.2 — MEDIUM

#### Hive 4.x 相对 Hive 3.x 的重大变更

**1. 废弃/移除的功能**

Hive 4.0 移除了以下对 Spark 集成有影响的功能:

| 移除项 | 影响 |
| ------ | ---- |
| MR 执行引擎 | 不影响 Spark (Spark 用自己引擎), 但影响 Hive 端的 UDF 兼容性 |
| Tez 引擎部分 API | 如 Hive 端使用 Tez, Spark 通过 Hive Metastore 交互不受影响 |
| 旧版 Hive CLI (`hive` 命令) | 仅影响测试时的手工操作, 改用 beeline |
| 部分 Hive 内置 UDF | 如果 Spark SQL 中调用这些 UDF, 可能报 `UDFClassNotFoundException` |
| `hive.mapred.mode` 相关配置 | 不影响 |
| 旧版 Lock Manager | Hive 4 使用新的 DbLockManager, 注意与 Hive ACID 表的锁交互 |

**2. Hive Metastore API 变更**

Hive 4.2.0 的 Metastore Thrift API 有新增字段和方法。Spark 4.1.2 使用的 `hive-metastore` 客户端版本可能与 Hive 4.2.0 的服务端不完全一致:

```properties
# Spark 4 默认使用 hive-metastore 3.1.x 的 client
# 连接 Hive 4.2.0 Metastore 时:
# 如果 API 向前兼容 → OK
# 如果新增必填字段 → Thrift 序列化错误
spark.sql.hive.metastore.version = 4.0
```

**重点测试**: 分别用 `spark.sql.hive.metastore.version=3.1` 和 `spark.sql.hive.metastore.version=4.0` 连接 Hive 4.2.0 Metastore，验证所有 DDL 操作是否正常。

**3. Hive 4 的 Iceberg 原生 Storage Handler**

Hive 4.x 通过 `STORED BY ICEBERG` 原生创建 Iceberg 表。Spark 4 通过 `USING ICEBERG` + `SparkCatalog` 创建。两种 Iceberg 表在 Metastore 中的元数据格式不同，必须验证互通性。

**4. Hive 4 ACID v2**

Hive 4 使用 ACID v2，与 Hive 3 的 ACID v1 不兼容。如果 Hive 3.1.3 升级到 Hive 4.2.0，原有的 ACID 表需要迁移。

**5. Spark 4 对 Hive 4 的支持状态**

SPARK-51348 "Upgrade Hive to 4+" 曾是一个进行中的工作项，Spark 4.1.2 的具体 Hive 4 支持程度需要实测确认。

#### 对 Hive 4.2.0 的重点测试

| 优先级 | 测试项 | 风险说明 |
| ------ | ------ | -------- |
| P0 | 用 `metastore-version=4.0` 连接 Hive 4.2.0 Metastore | Client/Server Thrift 兼容 |
| P0 | Hive 4 ACID v2 表的 Spark 读取 | ACID v2 的 reader 支持 |
| P0 | Hive 4 `STORED BY ICEBERG` 表 vs Spark `USING ICEBERG` 表的互读 | 元数据格式差异 |
| P1 | Hive 4 新 UDF/内置函数在 Spark SQL 中的可用性 | UDF 注册失败 |
| P1 | Hive 4 Materialized View 的 Spark 查询 | 查询重写兼容性 |
| P1 | Hive 4 Metastore 的分区统计信息 API | Thrift API 兼容 |
| P2 | 多 Catalog 配置 (Hive 4 + Iceberg + Paimon 同时注册) | 总扩展点冲突 |

---

### D.6 🟢 Iceberg 1.6 + Spark 4.1.2 — LOW

Iceberg 1.6 的生态矩阵明确包含 Spark 4.0。主要注意:

- Spark 4 需要使用 `iceberg-spark-runtime-4.0_2.13` (而非 3.5 的 runtime)
- Procedure 调用语法在 Spark 4 中可能有微调 (基于 ANTLR 4 解析器)
- Iceberg 1.6 的 `VARIANT` 类型支持可能与 Spark 4 的 `VARIANT` 类型行为有差异

#### 仍需验证的点

| 优先级 | 测试项 | 说明 |
| ------ | ------ | ---- |
| P1 | `CALL system.xxx` procedure 语法 | ANTLR 4 解析器兼容 |
| P1 | Iceberg time travel SQL 语法 (`VERSION AS OF`) | Spark 4 语法兼容 |
| P2 | Iceberg `VARIANT` 类型 | Spark 4 新类型 vs Iceberg 1.6 实现 |
| P2 | WAP (Write-Audit-Publish) 分支工作流 | Iceberg 1.6 新特性 |

---

### D.7 Spark 4 自身的 Breaking Changes (影响所有组件)

无论具体 Hive/Iceberg/Paimon 版本如何，Spark 4 自身引入了以下可能影响测试的变更:

**1. ANTLR 4 SQL Parser (最广泛影响)**

Spark 4 使用 ANTLR 4 而非 ANTLR 3 作为 SQL 解析器。这意味着:

- **自定义 SQL 扩展语法可能失效**: 如果 Iceberg/Paimon 的 `SparkSessionExtensions` 注入了自定义 Parser 规则（Spark 3.x 的 `ParserInterface`），在 Spark 4 中可能因接口变化而无法注入。
- **某些合法的 Hive SQL 语法在 Spark 4 中可能解析失败**: 特别是使用了 Hive 特有的 QL 语法。
- **注释语法变化**: 如 `--` 注释的行为可能有微调。

```sql
-- 在 Spark 4 中需重点验证的 SQL 语法:
-- 1. Hive 风格的 INSERT ... VALUES 多行
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- 2. Hive 风格的多表 INSERT
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.* WHERE src.key >= 100;

-- 3. LATERAL VIEW EXPLODE
SELECT id, col FROM t LATERAL VIEW EXPLODE(arr) AS col;
```

**2. 已移除的 Deprecated API**

| Spark 3.x 中可用, Spark 4 中移除 | 可能影响 |
| --------------------------------- | -------- |
| `spark.sql.legacy.xxx` 大量配置项 | 行为变化, 特别是时间/日期解析 |
| 部分 DataFrame SQL 字符串 API | `spark.sql("...")` 可能报 deprecation warning |
| `RDD.toLocalIterator` 某些重载 | 测试用代码需调整 |
| `SQLContext` (已被 `SparkSession` 替代) | 旧测试脚本需更新 |
| `HiveContext` | 必须用 `SparkSession` + `enableHiveSupport()` |

**3. ANSI 模式默认行为**

Spark 4 可能改变了某些 ANSI SQL 模式的默认值。例如:
- 除零操作: 返回 NULL vs 抛出异常
- 溢出处理: 自动升级类型 vs 抛出异常
- 字符串 cast 到数字: 返回 NULL vs 抛出异常

```sql
-- 重点验证:
SELECT CAST('abc' AS INT);  -- Spark 3: NULL, Spark 4 ANSI: 可能抛异常
SELECT 1/0;                 -- 行为可能变更
```

**4. Scala 2.13 / Java 17 要求**

- 所有依赖 JAR 必须编译为 Java 17 兼容的 class file version
- 如果某个依赖 JAR 是 Java 8/11 编译的，一般兼容，但反射调用可能受影响
- Scala 2.12 编译的 JAR 在 Scala 2.13 环境中可能因标准库变化而崩溃

---

### D.8 测试优先级排序 (综合建议)

按风险从高到低排列的**必须优先测试**的 20 个场景:

```
P0 — 阻塞级 (必须先验，失败则后续测试无意义)
─────────────────────────────────────────────
T-01  环境连通: Spark SQL 连接 Hive3/Hive4 Metastore
T-02  Paimon Catalog 注册: SHOW CATALOGS 含 paimon_catalog
T-03  Paimon 基础建表: CREATE TABLE paimon_catalog.db.test (id BIGINT, name STRING)
T-04  Paimon 数据写入: INSERT INTO paimon_catalog.db.test VALUES (1, 'a')
T-05  Iceberg 基础建表: CREATE TABLE iceberg_catalog.db.test (...) USING ICEBERG
T-06  Iceberg Catalog Procedure: CALL iceberg_catalog.system.current_snapshot('db.test')

P1 — 关键级 (决定核心功能是否可用)
─────────────────────────────────────────────
T-07  Hive ACID 表在 Spark4 中的读取 (Hive3/Hive4 分别测)
T-08  Hive ACID 表在 Spark4 中的 UPDATE/DELETE (Hive3/Hive4 分别测)
T-09  Hive4 原生 STORED BY ICEBERG 表 → Spark4 Iceberg Catalog 读取
T-10  Spark4 Iceberg UPDATE/DELETE/MERGE
T-11  Paimon UPSERT (INSERT same PK twice)
T-12  Paimon DELETE
T-13  跨 Catalog 查询: Hive table JOIN Iceberg table JOIN Paimon table
T-14  Iceberg Time Travel (VERSION AS OF / TIMESTAMP AS OF)

P2 — 重要级 (影响生产可靠性)
─────────────────────────────────────────────
T-15  Schema Evolution: Iceberg + Paimon 的 ADD/DROP/RENAME COLUMN
T-16  Parquet/ORC 所有压缩编码的读写一致性验证
T-17  Partition Evolution (Iceberg)
T-18  Paimon partial-update merge engine
T-19  外部表 DROP 后数据保留 + 重建表读取
T-20  数据类型边界: DECIMAL(38,18), 超大 STRING, UTF-8/Emoji, TIMESTAMP 时区
```

---

### D.9 版本升级建议路径

基于以上风险分析，推荐版本调整:

```
当前组合                            →  推荐组合
─────────────────────────────────────────────────────────
Spark     4.1.2                     →  保持不变 (最新)
Hive      3.1.3 (保留作对比)        →  保留, 用于 ACID v1 兼容性验证
Hive      4.2.0                     →  保持不变 (最新)
Iceberg   1.6                        →  升级到 1.9.x (与 Hive 4.2 内嵌版本对齐)
Paimon    0.8.2                      →  升级到 0.9.0 或 1.0+ (添加 Spark 4 模块支持)

升级后预期收益:
  - Paimon: 解决 CRITICAL 级别的 DataSource V2 API 不兼容
  - Iceberg: 消除与 Hive 4 内嵌 Iceberg 的版本冲突风险  
  - Hive 3.1.3: 作为 ACID v1 和旧版 UDF 的兼容性对照组保留
```

---

> **D.10 测试日志记录要点**: 对于每个失败的测试用例，请记录:
> 1. 完整的报错栈 (class name + method name + exception type)
> 2. Hive Metastore 版本 (`spark.sql.hive.metastore.version` 的值)
> 3. 使用的 Iceberg/Paimon runtime JAR 文件名和版本号
> 4. 是否启用了 ANSI 模式 (`spark.sql.ansi.enabled`)
> 5. Scala 版本 (`spark-submit --version` 输出)
>
> 这些信息对于判断是版本问题是配置问题至关重要。
