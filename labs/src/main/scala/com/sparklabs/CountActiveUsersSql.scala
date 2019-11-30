package com.sparklabs

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, substring}

object CountActiveUsersSql {
  def main(args: Array[String]): Unit = {
    var input = "src/resources/posts.csv"
    var output = "src/resources/count_active_users"
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
      .toDF("id", "userId", "title", "creationDate")
      .withColumn("date", substring(col("creationDate"), 1, 10))
      .createOrReplaceTempView("users")

    val results = spark.sql("SELECT date, count(distinct userId) FROM users GROUP BY date")
      .toDF("date", "userIds")

    results.repartition(1).write.option("header", "true").option("delimiter", ",").mode(SaveMode.Overwrite).csv(output)


  }
}
