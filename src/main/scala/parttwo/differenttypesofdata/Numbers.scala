package parttwo.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Numbers extends App {

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

  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

  // rounding
  df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
  // you can use bround to round to a lower number

  df.stat.corr("Quantity", "UnitPrice")
  df.select(corr("Quantity", "UnitPrice")).show(2)

  // compute summary statistics for a column or set of columns
  df.describe().show()

  // calculate either exact or approximate quantiles of data
  val colName = "UnitPrice"
  val quantileProbs = Array(0.5)
  val relError = 0.05
  df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51


}
