package com.xpand.it

import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkApp {
  def main(args: Array[String]): Unit = {
    println("Starting....")

    //disable logs
    Logger.getLogger("org").setLevel(Level.ERROR)

    //setup
    val spark = SparkSession.builder()
      .appName("Xpand It Challenge")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    //imported csv
    val googleUserReviews:DataFrame = spark.read.option("header", value = true).csv("googleplaystore_user_reviews.csv")
    val googlePlayStore:DataFrame = spark.read.option("header", value = true).csv("googleplaystore.csv")

    println("--------------------------------------------------Part 1--------------------------------------------------")
    var df_1:DataFrame = partOne(googleUserReviews)

    df_1.printSchema()
    df_1.show()


    println("--------------------------------------------------Part 2--------------------------------------------------")
    var df_2:DataFrame = partTwo(googlePlayStore, "§", "Ex2/", "best_apps.csv", spark)

    df_2.printSchema()
    df_2.show()


    println("--------------------------------------------------Part 3--------------------------------------------------")
    var df_3:DataFrame = partThree(googlePlayStore)

    df_3.printSchema()
    df_3.show()


    println("--------------------------------------------------Part 4--------------------------------------------------")
    var joinedDf_3_1:DataFrame = partFour(df_3, df_1, "gzip", "Ex4/", "googleplaystore_cleaned.parquet", spark)

    joinedDf_3_1.printSchema()
    joinedDf_3_1.show()


    println("--------------------------------------------------Part 4--------------------------------------------------")
    //TODO: var df_4 = partFive(df_3)


    println("Quitting....")
    spark.close()
  }

  //Part 1
  def partOne(df:DataFrame): DataFrame = {

    var df_1:DataFrame = df
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
   * @param spark spark
   * @return a Boolean:
   * -> true - if file exists
   * -> false - if file doesn't exist
   */
  def doesFileExists(pathToFile:String, fileName:String, spark:SparkSession): Boolean = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    //exists or doesnt exist
    val fileExists = fs.exists(new Path(pathToFile+fileName))

    return fileExists
  }

  /**
   * renames a file
   * @param pathToFile path of the file
   * @param fileName wanted name to be written
   * @param spark spark
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

    var df_2:DataFrame = df
      .withColumn("Rating", df("Rating").cast(DoubleType))
      .filter(df("Rating") >= 4.0 && !isnan(df("Rating")))
      .sort(desc("Rating"))

    writeFile(df_2, "csv", delim, pathToFile, fileName, spark)

    return df_2
  }

  //Part 3
  def partThree(df:DataFrame): DataFrame = {

    var df_3:DataFrame = df.sort(desc("Reviews"))
      .groupBy("App")
      .agg(
        collect_set("Category").as("Categories"),
        //first("Rating").cast(LongType).as("Rating"),
        first("Rating").as("Rating"),
        first("Reviews").cast(LongType).as("Reviews"),
        first("Size").as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        first("Price").as("Price"),
        first("Content Rating").as("Content_Rating"),
        split(first("Genres"), ";").as("Genres"), //splits by ";"
        first("Last Updated").as("Last_Updated"),
        first("Current Ver").as("Current_Version"),
        first("Android Ver").as("Minimum_Android_Version")
      )
      .na.fill(0, Seq("Reviews"))

    //making it double type
    df_3 = df_3.withColumn("Rating", col("Rating").cast(DoubleType))
    df_3 = df_3.withColumn("Rating", when(col("Rating").isNaN,lit(null))
      .otherwise(col("Rating")))

    //make them as zero of long type
    df_3 = df_3.withColumn("Reviews", when(col("Reviews").isNull || col("Reviews").isNaN ,0L)
      .otherwise(col("Reviews"))) //Valor por defeito = 0 -> Se existir NaN ou Null é retirado.

    //check if is Mb or Kb and making according transformation (remove the letters M and K)
    df_3 = df_3.withColumn("Size",
      when(
        col("size").endsWith("M"), split(col("Size"), "M").getItem(0)
      )
        .when(
          col("Size").endsWith("K"), split(col("Size"), "k").getItem(0)./(scala.math.pow(10, 3))
        )
        .otherwise(null)
    )
    df_3 = df_3.withColumn("Size", col("Size").cast(DoubleType))

    //remove the dollar symbol, make value as double type and change to *0.9
    df_3 = df_3.withColumn("Price", regexp_replace(col("Price"), "$", ""))
    df_3 = df_3.withColumn("Price", col("Price").cast(DoubleType))
    df_3 = df_3.withColumn("Price", col("Price") * 0.9)

    //remove the "and up"
    df_3 = df_3.withColumn("Minimum_Android_Version", regexp_replace(col("Minimum_Android_Version"), " and up", ""))

    return df_3
  }

  //Part 4
  def partFour(df_3:DataFrame, df_1:DataFrame, compressionMethod:String, pathToFile:String, fileName:String, spark:SparkSession): DataFrame = {

    var joinedDf:DataFrame = df_3.join(df_1, Seq("App"))

    writeFile(joinedDf, "parquet", compressionMethod, pathToFile, fileName, spark)

    return joinedDf
  }

  /**
   * writes a file
   * @param df dataframe
   * @param extension the extension of the file
   * @param typeOfOperation type of option like delimiter or compression
   * @param pathToFile path of the file
   */
  def writeFile(df: DataFrame, extension:String, typeOfOperation:String, pathToFile:String, fileName:String, spark:SparkSession): Unit = {

    val ogName:String = "part*"

    val fileExists:Boolean = doesFileExists(pathToFile, ogName, spark)

    if (!fileExists) {
      extension match {
        case "csv" => df.coalesce(1).write.option("delimiter", typeOfOperation).csv(pathToFile)
        case "parquet" => df.coalesce(1).write.option("compression", typeOfOperation).parquet(pathToFile)
      }
      renameWrittenFile(pathToFile, fileName, spark)
    }
  }

  //Part 5
  def PartFive(df:DataFrame): DataFrame ={

    var df_4:DataFrame = df

    return df_4
  }
}
