package parttwo.basicstructuredoperations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Basics extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // Defining a manual custom schema and enforce it on a DataFrame
  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  val df = spark.read.format("json").schema(myManualSchema)
    .load("src/main/resources/data/flight-data/json/2015-summary.json")

  println(df.schema)

  // define new column
  // col(someColumnName)


  // See first row
  println(df.first())
  // Create new row
  val myRow = Row("Hello", null, 1, false)


}
