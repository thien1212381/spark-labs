package com.sparklabs

import org.apache.spark.sql.functions.{col, countDistinct, to_date, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SaveMode, SparkSession}

object CountActiveUsers {
  def main(args: Array[String]): Unit = {
    var input = "src/resources/posts.csv"
    var output = "src/output/count_active_users"
    var master = "local[*]"

    if (args.length >= 2) {
      master = "yarn"
      input = args(0)
      output = args(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Count Active Users Daily")
      .master(master)
      .getOrCreate()

    spark
      .read
      .option("delimiter", "\t")
      .csv(input)
      .toDF("id", "user_id", "title", "created_at")
      .withColumn("created_at", unix_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(TimestampType))
      .withColumn("date", to_date(col("created_at")))
      .groupBy("date")
      .agg(countDistinct("user_id").as("countDistinct"))
      .orderBy(col("date").asc)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(output)

  }
}
