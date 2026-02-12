from os import environ as env

from pyspark.sql import SparkSession


def bronze_etl():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("economy_bronze")
             .getOrCreate())

    inflation_bronze_data_frame = (spark
                                   .read
                                   .option("header", "true")
                                   .option("inferSchema", "true")
                                   .csv(env.get("INFLATION_DATA")))

    interest_rate_bronze_data_frame = (spark
                                       .read
                                       .option("header", "true")
                                       .option("inferSchema", "true")
                                       .csv(env.get("INTEREST_RATE_DATA")))

    (inflation_bronze_data_frame
     .write
     .mode("overwrite")
     .parquet(env.get("INFLATION_MEDALLION_BRONZE")))

    (interest_rate_bronze_data_frame
     .write
     .mode("overwrite")
     .parquet(env.get("INTEREST_RATE_MEDALLION_BRONZE")))

    spark.stop()


if __name__ == "__main__":
    bronze_etl()
