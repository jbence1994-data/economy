from os import environ as env

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag


def silver_etl():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("economy_silver")
             .getOrCreate())

    inflation_silver_data_frame = (spark
                                   .read
                                   .parquet(env.get("INFLATION_MEDALLION_BRONZE")))

    interest_rate_silver_data_frame = (spark
                                       .read
                                       .parquet(env.get("INTEREST_RATE_MEDALLION_BRONZE")))

    inflation_silver_data_frame = (inflation_silver_data_frame.withColumn(
        colName="change",
        col=col("value") - lag("value").over(Window.orderBy("year", "month"))
    ))

    interest_rate_silver_data_frame = (interest_rate_silver_data_frame.withColumn(
        colName="change",
        col=col("value") - lag("value").over(Window.orderBy("year", "month", "day"))
    ))

    (inflation_silver_data_frame
     .write
     .mode("overwrite")
     .partitionBy("year")
     .parquet(env.get("INFLATION_MEDALLION_SILVER")))

    (interest_rate_silver_data_frame
     .write
     .mode("overwrite")
     .partitionBy("year")
     .parquet(env.get("INTEREST_RATE_MEDALLION_SILVER")))

    spark.stop()


if __name__ == "__main__":
    silver_etl()
