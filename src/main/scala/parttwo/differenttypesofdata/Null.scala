package parttwo.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Null extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/retail-data/by-day/2010-12-01.csv")
  df.printSchema()
  df.createOrReplaceTempView("dfTable")


  // coalesce function allows us to select the first non-null value from
  // a set of columns
  df.select(coalesce(col("Description"), col("CustomerId"))).show()

  // any drops a row if the values are null, "all" drops the row only if all values are null or NaN for that row
  df.na.drop("any")
  df.na.drop("all")

  df.na.drop("all", Seq("StockCode", "InvoiceNo"))

  // fill function
  df.na.fill("All Null values become this string")

  // with Integer
  df.na.fill(5, Seq("StockCode", "InvoiceNo"))

  // with scala Map
  val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
  df.na.fill(fillColValues)

  // replace
  df.na.replace("Description", Map("" -> "UNKNOWN"))




}
