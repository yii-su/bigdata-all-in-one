# spark-submit --master local --deploy-mode client test0.py

from pyspark.sql import SparkSession

# 创建SparkSession并启用Hive支持
spark = SparkSession.builder \
    .appName("PySparkHiveExample") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("""
    select * from tdb1.test_table
""").show()

spark.stop()


# spark.sql("""
#     CREATE DATABASE tdb1;
# """).show()

# spark.sql("""
#     CREATE TABLE tdb1.test_table (id INT, name STRING);
# """)
#     # INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');