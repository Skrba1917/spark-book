package part2.differenttypesofdata

import breeze.linalg.InjectNumericOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Booleans extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/retail-data/by-day/2010-12-01.csv")
  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  // === or =!= to check also works but I used equalTo() here
  df.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo", "Description")
    .show(5, false)

  // this also works in string expression
  df.where("InvoiceNo = 536365")
    .show(5, false)

  val priceFiler = col("UnitPrice") > 600
  val descripFilter = col("Description").contains("POSTAGE")

  df.where(col("StockCode").isin("DOT")).where(priceFiler.or(descripFilter))
    .show()

  // we can filter dataframes and based on boolean select specific rows that have desired value




}
