package sparktoolset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredStreaming extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/retail-data/by-day/*.csv")

  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema

  staticDataFrame
    .selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day")
    )
    .sum("total_cost")


  //spark.conf.set("spark.sql.shuffle.partitions", "5") specifies number of partitions

  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load("src/main/resources/data/retail-data/by-dat/*.csv")

  //streamingDataFrame.isStreaming - returns true

  val purchaseByCustomerPerHour = streamingDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate"
    )
    .groupBy(
      $"CustomerId", window ($"InvoiceDate", "1 day")
    ).sum("total_cost")

  purchaseByCustomerPerHour.writeStream
    .format("memory") // we can use "console" instead of "memory" if we want to see the output in console
    .queryName("customer_purchases")
    .outputMode("complete")
    .start()

  spark.sql("select * from customer_purchases order by `sum(total_cost)` DESC").show(5)

  // these streaming methods shouldn't be used in production, it's just a demonstration

  

}
