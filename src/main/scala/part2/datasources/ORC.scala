package part2.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ORC extends App {

  val spark = SparkSession.builder()
    .appName("ORC")
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

  // reading orc files
  spark.read.format("orc")
    .load("src/main/resources/data/flight-data/orc/2010-summary.orc")

  // writing orc files
  csvFile.write.format("orc")
    .mode("overwrite")
    .save("src/main/resources/data/tmp/my-orc-file.orc")

}
