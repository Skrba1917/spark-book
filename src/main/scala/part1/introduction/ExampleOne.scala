package introduction

import org.apache.spark.sql.SparkSession

object ExampleOne extends App {

  val spark = SparkSession.builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val flightData2015 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/flight-data/csv/2015-summary.csv")

  //flightData2015.show()


  // SHOWS A PLAN to see dataframe's lineage
  flightData2015.sort("count").explain()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  flightData2015.sort("count").take(2)

}
