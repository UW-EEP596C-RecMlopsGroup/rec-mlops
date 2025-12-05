# 文件位置: src/init_delta_tables.py
import os
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def init_delta_tables():
    print("🚀 Starting Delta Lake initialization inside Docker...")

    # 1. 配置 Spark + Delta
    builder = (
        SparkSession.builder.appName("DeltaSetup")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # 注意：这里我们使用 /tmp/delta-tables，与 setup.py 保持一致
    # 但建议生产环境改为 /data/delta-tables 以便持久化
    delta_path = "/tmp/delta-tables"
    os.makedirs(delta_path, exist_ok=True)

    # 2. 创建 Interactions 表
    print("📦 Creating interactions table...")
    interactions_schema = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("item_id", LongType(), True),
            StructField("rating", DoubleType(), True),
            StructField("interaction_type", StringType(), True),
            StructField("timestamp", DoubleType(), True),
            StructField("session_id", StringType(), True),
        ]
    )

    # 生成一些样本数据
    sample_data = []
    for i in range(1000):
        sample_data.append(
            (
                int(i % 20),  # user_id
                int(i % 50),  # item_id
                float(3.0 + (i % 2)),  # rating
                "rating",  # interaction_type
                float(time.time()),  # timestamp
                f"session_{i}",  # session_id
            )
        )

    df = spark.createDataFrame(sample_data, interactions_schema)

    # 写入 Delta Lake
    df.write.format("delta").mode("overwrite").save(f"{delta_path}/interactions")
    print(f"✅ Interactions table created at {delta_path}/interactions")

    # 3. 创建 User Profiles 表
    print("👤 Creating user_profiles table...")
    user_schema = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("avg_rating", DoubleType(), True),
            StructField("interaction_count", LongType(), True),
            StructField("last_interaction", DoubleType(), True),
        ]
    )

    # 创建空表或样本数据
    user_data = [(1, 4.5, 10, float(time.time()))]
    user_df = spark.createDataFrame(user_data, user_schema)

    user_df.write.format("delta").mode("overwrite").save(f"{delta_path}/user_profiles")
    print(f"✅ User profiles table created at {delta_path}/user_profiles")

    spark.stop()
    print("🎉 Initialization complete!")


if __name__ == "__main__":
    init_delta_tables()
