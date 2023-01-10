package introduction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DfAndSQL extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val flightData2015 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/flight-data/csv/2015-summary.csv")

  flightData2015.createOrReplaceTempView("flight_data_2015")

  flightData2015.show()

  val sqlWay = spark.sql(
    "select dest_country_name, count(1) from flight_data_2015 group by dest_country_name"
  )
  sqlWay.show()

  val dataFrameWay = flightData2015
    .groupBy('DEST_COUNTRY_NAME)
    .count()

  sqlWay.explain()
  dataFrameWay.explain()

  // SQL WAY
  val maxSql = spark.sql("""
  SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
  FROM flight_data_2015
  GROUP BY DEST_COUNTRY_NAME
  ORDER BY sum(count) DESC
  LIMIT 5
  """)

  maxSql.show()

  // DATAFRAME WAY
  flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "destination_total")
    .sort(desc("destination_total"))
    .limit(5)
    .show()

}
