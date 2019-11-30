package com.sparklabs

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, substring}

object CountActiveUsersSql {
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
      .appName("Spark Count Active Users Daily SQL")
      .master(master)
      .getOrCreate()

    spark.read.option("delimiter", "\t").csv(input)
      .toDF("id", "user_id", "title", "created_at")
      .withColumn("date", substring(col("created_at"), 1, 10))
      .createOrReplaceTempView("posts")

    val sql = "SELECT date, count(distinct user_id) FROM posts GROUP BY date"
    // count distinct using approx_count_distinct
    // val sql = "SELECT date, approx_count_distinct(user_id) FROM posts GROUP BY date"
    val results = spark.sql(sql)
      .toDF("date", "userIds")

    results.repartition(1).write.option("header", "true").option("delimiter", ",").mode(SaveMode.Overwrite).csv(output)


  }
}