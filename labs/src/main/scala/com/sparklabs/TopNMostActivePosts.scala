package com.sparklabs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class PostCommentCount(month: String, postId: String, commentCount: Long)
case class Result(month: String, postIds: String)

object TopNMostActivePosts {
  def main(args: Array[String]): Unit = {
    var inputPosts = "src/resources/posts.csv"
    var inputComments = "src/resources/comments.csv"
    var output = "src/output/top_n"
    var master = "local[*]"

    if (args.length >= 2) {
      master = "yarn"
      inputPosts = args(0)
      inputComments = args(1)
      output = args(2)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Count Active Users Daily")
      .master(master)
      .getOrCreate()

    val postsDF = spark
      .read
      .option("delimiter", "\t")
      .csv(inputPosts)
      .toDF("id", "userId", "title", "creationDate")
      .withColumn("month", substring(col("creationDate"), 1, 7))

    val commentsDF = spark
      .read
      .option("delimiter", "\t")
      .csv(inputComments)
      .toDF("id", "userId", "postId", "creationDate")
      .withColumn("month", substring(col("creationDate"), 1, 7))

    import spark.implicits._

    val postCommentCountDS = postsDF.join(commentsDF, postsDF("month") === commentsDF("month") && postsDF("id") === commentsDF("postId"))
      .groupBy(postsDF("month"), postsDF("id").as("postId"))
      .agg(count(commentsDF("id")).as("commentCount"))
      .as[PostCommentCount]

    val results = postCommentCountDS.groupByKey(p => p.month).mapGroups {
      case (month, values) => {

        // Sort comment count desc
        val sortedCommentCount = values.toSeq.sortWith((a, b) => a.commentCount > b.commentCount).toBuffer

        val top20 = if (sortedCommentCount.length <= 20) sortedCommentCount else sortedCommentCount.take(20)

        val postIds = top20.map(a => a.commentCount).mkString(",")

        Result(month, postIds)
      }
    }

    results.repartition(1).write.option("header", "true").option("delimiter", "\t").csv(output)
  }
}
