package part2.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CSV extends App {

  val spark = SparkSession.builder()
    .appName("CSV")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

//  spark.read.format("csv")
//    .option("header", "true")
//    .option("mode", "FAILFAST")
//    .option("inferSchema", "true")
//    .load("src/path/to/file.csv")

  // we can create manual schema
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

  // WRITE CSV FILES

  csvFile.write.format("csv")
    .mode("overwrite")
    .option("sep", "\t")
    .save("src/main/resources/data/tmp/my-tsv-file.tsv")


}
