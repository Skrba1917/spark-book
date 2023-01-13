package part2.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object JSON extends App {

  val spark = SparkSession.builder()
    .appName("JSON")
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

  // reading json files

  spark.read.format("json")
    .option("mode", "FAILFAST")
    .schema(myManualSchema)
    .load("src/main/resources/data/flight-data/json/2010-summary.json")

  // writing json files
  csvFile.write.format("json")
    .mode("overwrite")
    .save("src/main/resources/data/tmp/my-json-file.json")


}
