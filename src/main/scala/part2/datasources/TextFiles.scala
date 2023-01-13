package part2.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object TextFiles extends App {

  val spark = SparkSession.builder()
    .appName("TxtFiles")
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

  // reading txt files

  spark.read.textFile("src/main/resources/data/flight-data/csv/2010-summary.csv")
    .selectExpr("split(value, ',') as rows")

  // writing txt files
  csvFile.select("DEST_COUNTRY_NAME").write.text("/tmp/simple-text-file.txt")

  csvFile.limit(10).selectExpr("DEST_COUNTRY_NAME", "count")
    .write.partitionBy("count").text("src/main/resources/data/tmp/five-csv-files2.csv")
}
