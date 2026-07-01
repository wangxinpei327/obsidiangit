Though KSHC is a pure Spark DataSource V2 connector which isn’t coupled with Kyuubi deployment, due to the implementation inside `spark-sql`, you should not expect KSHC works properly with `spark-sql`, and any issues caused by such a combination usage won’t be considered at this time. Instead,<u> **it’s recommended using BeeLine with Kyuubi as a drop-in replacement for `spark-sql`, or switching to `spark-shell`.**</u>

KSHC supports accessing Kerberized Hive Metastore and HDFS, by using keytab, or TGT cache, or Delegation Token. It’s not expected to work properly with multiple KDC instances, the limitation comes from JDK Krb5LoginModule, for such cases, consider setting up Cross-Realm Kerberos trusts, then you just need to talk with one KDC.

For HMS Thrift API used by Spark, it’s known that Hive 2.3.9 client is compatible with HMS from 2.1 to 4.0, and Hive 2.3.10 client is compatible with HMS from 1.1 to 4.0, such version combinations should cover the most cases. For other corner cases, ==**KSHC also supports `spark.sql.catalog.<catalog_name>.spark.sql.hive.metastore.jars` and `spark.sql.catalog.<catalog_name>.spark.sql.hive.metastore.version` as well as the Spark built-in Hive datasource does, you can refer to the Spark documentation for details.**==

Currently, KSHC has not implemented the Parquet/ORC Hive tables read/write optimization, in other words, it always uses Hive SerDe to access Hive tables, so there might be a performance gap compared to the Spark built-in Hive datasource, especially due to lack of support for vectorized reading. ==And you may hit bugs caused by Hive SerDe, e.g. `ParquetHiveSerDe` can not read Parquet files that decimals are written in int-based format produced by Spark Parquet datasource writer with `spark.sql.parquet.writeLegacyFormat=false`.==

[

](https://kyuubi.readthedocs.io/en/master/connector/spark/kudu.html "previous page")