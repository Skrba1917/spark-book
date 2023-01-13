package part2.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Parquet extends App {

  val spark = SparkSession.builder()
    .appName("Parquet")
    .config("spark.master", "local")
    .getOrCreate()

  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)
  ))

  val csvFile = spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(myManualSchema)
    .load("src/main/resources/data/flight-data/csv/2010-summary.csv")

  // read parquet files
  // parquet enforces its own schema
  spark.read.format("parquet")
    .load("src/main/resources/data/flight-data/parquet/2010-summary.parquet")

  // writing parquet files
  csvFile.write.format("parquet")
    .mode("overwrite")
    .save("src/main/resources/data/tmp/my-parquet-file.parquet")





}
