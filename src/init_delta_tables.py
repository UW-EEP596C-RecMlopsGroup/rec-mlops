# Location: src/init_delta_tables.py
import os
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def init_delta_tables():
    print("🚀 Starting Delta Lake initialization inside Docker...")

    # 1. Configure Spark + Delta
    builder = (
        SparkSession.builder.appName("DeltaSetup")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Note: use /tmp/delta-tables here to match setup.py
    # For production, switch to /data/delta-tables for persistence
    delta_path = "/tmp/delta-tables"
    os.makedirs(delta_path, exist_ok=True)

    # 2. Create the interactions table
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

    # Generate sample rows
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

    # Write to Delta Lake
    df.write.format("delta").mode("overwrite").save(f"{delta_path}/interactions")
    print(f"✅ Interactions table created at {delta_path}/interactions")

    # 3. Create the user_profiles table
    print("👤 Creating user_profiles table...")
    user_schema = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("avg_rating", DoubleType(), True),
            StructField("interaction_count", LongType(), True),
            StructField("last_interaction", DoubleType(), True),
        ]
    )

    # Create an empty table or seed it with sample data
    user_data = [(1, 4.5, 10, float(time.time()))]
    user_df = spark.createDataFrame(user_data, user_schema)

    user_df.write.format("delta").mode("overwrite").save(f"{delta_path}/user_profiles")
    print(f"✅ User profiles table created at {delta_path}/user_profiles")

    spark.stop()
    print("🎉 Initialization complete!")


if __name__ == "__main__":
    init_delta_tables()
