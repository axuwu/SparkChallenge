package com.xpand.it

import java.io.File
import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkApp {
  def main(args: Array[String]): Unit = {
    println("Starting....")

    //disable logs
    Logger.getLogger("org").setLevel((Level.ERROR));

    //setup
    val spark = SparkSession.builder()
      .appName("Xpand It Challenge")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    //imported csv
    val googleUserReviews = spark.read.option("header", true).csv("googleplaystore_user_reviews.csv")
    val googlePlayStore = spark.read.option("header", true).csv("googleplaystore.csv")

    //Part 1
    var df_1: DataFrame = googleUserReviews
    df_1.printSchema()
    df_1.show()

    println("Quitting....")
    spark.close()
  }
}
