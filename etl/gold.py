from os import environ as env

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max


def gold_etl():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("economy_gold")
             .getOrCreate())

    inflation_gold_data_frame = (spark
                                 .read
                                 .parquet(env.get("INFLATION_MEDALLION_SILVER")))

    interest_rate_gold_data_frame = (spark
                                     .read
                                     .parquet(env.get("INTEREST_RATE_MEDALLION_SILVER")))

    inflation_gold_data_frame = inflation_gold_data_frame.groupBy("year").agg(
        min("value").alias("min_inflation"),
        max("value").alias("max_inflation")
    ).orderBy("year")

    interest_rate_gold_data_frame = interest_rate_gold_data_frame.groupBy("year").agg(
        min("value").alias("min_interest_rate"),
        max("value").alias("max_interest_rate")
    ).orderBy("year")

    (inflation_gold_data_frame
     .write
     .mode("overwrite")
     .parquet(env.get("INFLATION_MEDALLION_GOLD")))

    (interest_rate_gold_data_frame
     .write
     .mode("overwrite")
     .parquet(env.get("INTEREST_RATE_MEDALLION_GOLD")))

    spark.stop()


if __name__ == "__main__":
    gold_etl()
