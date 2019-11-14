package com.sparklabs

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, to_date, unix_timestamp, count}
import org.apache.spark.sql.types.TimestampType

object CountNumberOfPostRegion {
  def main(args: Array[String]): Unit = {
    var inputPosts = "src/resources/posts.csv"
    var inputUsers = "src/resources/users.csv"
    var output = "src/output/count_number_of_post_region"

    if (args.length == 4) {
      inputPosts = args(1)
      inputUsers = args(2)
      output = args(3)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Count Active Users Daily")
      .master("local[*]")
      .getOrCreate()

    val postsDF = spark
      .read
      .option("delimiter", "\t")
      .csv(inputPosts)
      .toDF("id", "userId", "title", "creationDate")

    val usersDF = spark
      .read
      .option("delimiter", "\t")
      .csv(inputUsers)
      .toDF("id", "region")

    postsDF
      .join(usersDF, postsDF("userId") === usersDF("id"))
      .select(postsDF("id"), postsDF("userId"), usersDF("region"), postsDF("creationDate"))
      .withColumn("createdAt", unix_timestamp(col("creationDate"), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(TimestampType))
      .withColumn("date", to_date(col("createdAt")))
      .groupBy("date")
      .pivot("region")
      .agg(count(postsDF("id")))
      .write
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .csv(output)
  }
}
