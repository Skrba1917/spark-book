package part2.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object PartitionAndBucketing extends App {

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

  // PARTITIONING allows us to control what data is store and where as we write it
  csvFile.limit(10).write.mode("overwrite")
    .partitionBy("DEST_COUNTRY_NAME")
    .save("src/main/resources/data/tmp/partitioned-files.parquet")

  // BUCKETING allows us to control the data that s specifically written to each file
  val numberBuckets = 10
  val columnToBucketBy = "count"
  csvFile.write.format("parquet").mode("overwrite")
    .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")


}
