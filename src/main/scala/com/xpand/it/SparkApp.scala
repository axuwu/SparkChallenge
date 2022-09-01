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
    val googleUserReviews:DataFrame = spark.read.option("header", true).csv("googleplaystore_user_reviews.csv")
    val googlePlayStore:DataFrame = spark.read.option("header", true).csv("googleplaystore.csv")

    println("--------------------------------------------------Part 1--------------------------------------------------")
    var df_1: DataFrame = partOne(googleUserReviews)

    df_1.printSchema()
    df_1.show()

    println("--------------------------------------------------Part 2--------------------------------------------------")
    var df_2 = partTwo(googlePlayStore, "§", "Ex2/", "best_apps.csv", spark)

    df_2.printSchema()
    df_2.show()


    println("Quitting....")
    spark.close()
  }

  //Part 1
  def partOne(df:DataFrame): DataFrame = {
    var df_1: DataFrame = df
      .groupBy("App")//groups by App
      .agg(avg(df("Sentiment_Polarity")).as("Average_Sentiment_Polarity")) //does the average of Polarity
      .withColumn("Average_Sentiment_Polarity", col("Average_Sentiment_Polarity").cast(DoubleType)) //turns them as Double
      .na.fill(0)// fills the NaN as 0.0

    return df_1
  }

  /**
   * To check if file exists
   * @param pathToFile path of the file
   * @param fileName file's name
   * @param spark
   * @return a Boolean:
   * -> true - if file exists
   * -> false - if file doesn't exist
   */
  def doesFileExists(pathToFile:String, fileName:String, spark:SparkSession): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(pathToFile+fileName))
    return fileExists
  }

  /**
   * writes a csv
   * @param df dataframe wanted to be exported as csv
   * @param delim delimiter
   * @param pathToFile path of the file
   */
  def writeCSV(df: DataFrame, delim:String, pathToFile:String): Unit = {
    df.coalesce(1).write
      .option("delimiter", delim)
      .csv(pathToFile)
  }

  /**
   * renames a file
   * @param pathToFile path of the file
   * @param fileName wanted name to be written
   * @param spark
   */
  def renameWrittenFile(pathToFile:String, fileName:String, spark:SparkSession): Unit = {

    val ogName:String = "part*"

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val file = fs.globStatus(new Path(pathToFile+ogName))(0).getPath().getName()
    //renames that file to best_apps.csv
    fs.rename(new Path(pathToFile + file), new Path(pathToFile + fileName))
    //delete temp file
    fs.delete(new Path(fileName+"-temp"), true)
  }

  //Part 2
  def partTwo(df:DataFrame, delim:String, pathToFile:String, fileName:String, spark:SparkSession): DataFrame = {

    val ogName:String = "part*"

    var df_2 = df
      .withColumn("Rating", df("Rating").cast(DoubleType))
      .filter(df("Rating") >= 4.0 && !isnan(df("Rating")))
      .sort(desc("Rating"))

    //check if file exists
    val fileExists = doesFileExists(pathToFile, ogName, spark)

    if (!fileExists) {
      //writes the csv
      writeCSV(df_2, delim, pathToFile)
      //renames the csv
      renameWrittenFile(pathToFile, fileName, spark)
    }

    return df_2
  }

}
