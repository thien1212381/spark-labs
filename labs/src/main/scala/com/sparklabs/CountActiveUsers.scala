package com.sparklabs

import org.apache.spark.sql.functions.{col, countDistinct, to_date, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SaveMode, SparkSession}

object CountActiveUsers {
  def main(args: Array[String]): Unit = {
    var input = "src/resources/posts.csv"
    var output = "src/resources/count_active_users"

    if (args.length == 3) {
      input = args(1)
      output = args(2)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Count Active Users Daily")
      .master("local[*]")
      .getOrCreate()

    spark
      .read
      .option("delimiter", "\t")
      .csv(input)
      .toDF("id", "userId", "title", "creationDate")
      .withColumn("createdAt", unix_timestamp(col("creationDate"), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(TimestampType))
      .withColumn("date", to_date(col("createdAt")))
      .groupBy("date")
      .agg(countDistinct("userId").as("countDistinct"))
      .orderBy(col("date").asc)
      .write
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .csv(output)

  }
}
