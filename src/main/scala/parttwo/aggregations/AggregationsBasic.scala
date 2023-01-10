package parttwo.aggregations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object AggregationsBasic extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/retail-data/all/*.csv")
    .coalesce(5)

  df.cache()
  df.createOrReplaceTempView("dfTable")
  df.show()

  println(df.count())

  // count

  df.select(count("StockCode")).show()

  // countDistinct
  // for selecting unique groups
  df.select(countDistinct("StockCode")).show()

  // approx_count_distinct
  df.select(approx_count_distinct("StockCode", 0.1)).show()

  // first and last
  df.select(first("StockCode"), last("StockCode")).show()

  // min and max
  df.select(min("Quantity"), max("Quantity")).show()

  // sum
  df.select(sum("Quantity")).show()

  // sumDistinct
  df.select(sumDistinct("Quantity")).show

  // avg
  df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases")
  ).selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases"
  ).show()

  // Variance and Standard Deviation

  df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"),
  stddev_samp("Quantity")).show()

  // skewness and kurtosis
  df.select(skewness("Quantity"), kurtosis("Quantity")).show()

  // Covariance and Correlation
  df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")
  ).show()

  // Aggregating to Complex Types
  df.agg(collect_set("Country"), collect_list("Country")).show()

  // Grouping
  df.groupBy("InvoiceNo", "CustomerId").count().show()

  // Grouping with Expressions
  df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()

  // Grouping with Maps
  df.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show()

  // Window Functions
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm"
  ))
  dfWithDate.createOrReplaceTempView("dfWithDate")

  val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

  val purchaseDenseRank = dense_rank().over(windowSpec)
  val purchaseRank = rank().over(windowSpec)

  dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
    .select(
      col("CustomerId"),
      col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity")
    ).show()

  // Grouping Sets
  val dfNoNull = dfWithDate.drop()
  dfNoNull.createOrReplaceTempView("dfNoNull")

  // Rollups
  val rolledUpDF = dfNoNull.rollup("Date", "Country")
    .agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity) as total_quantity`")
    .orderBy("Date")
  rolledUpDF.show()

  rolledUpDF.where("Country IS NULL").show()

  // Cube
  dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

  // Grouping Metadata
  dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
    .orderBy(expr("grouping_id()").desc)
    .show()

  // Pivot
  val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

  pivoted.where("date > '2011-12-05'").select("date", "`USA_sum(Quantity)`").show()

  // UDF
  class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", BooleanType) :: Nil)

    def bufferSchema: StructType = StructType(
      StructField("result", BooleanType) :: Nil
    )

    def dataType: DataType = BooleanType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = true
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }

    def evaluate(buffer: Row): Any = {
      buffer(0)
    }
  }

  val ba = new BoolAnd
  spark.udf.register("booland", ba)

  import org.apache.spark.sql.functions._

  spark.range(1)
    .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
    .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
    .select(ba(col("t")), expr("booland(f)"))
    .show()

}
