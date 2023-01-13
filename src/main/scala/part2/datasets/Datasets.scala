package part2.datasets

import org.apache.spark.sql.SparkSession

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // case classes
  case class Flight (
      DEST_COUNTRY_NAME: String,
      ORIGIN_COUNTRY_NAME: String,
      count: BigInt
                    )

  val flightsDF = spark.read
    .parquet("src/main/resources/data/flight-data/parquet/2010-summary.parquet")
  val flights = flightsDF.as[Flight]

  // actions
  flights.show(2)
  println(flights.first.DEST_COUNTRY_NAME)

  // filtering
  def originIsDestination(flight_row: Flight): Boolean = {
    return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
  }

  println(flights.filter(flight_row => originIsDestination(flight_row)).first())

  flights.collect().filter(flight_row => originIsDestination(flight_row)) // we get the same answer as before

  // mapping
  val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
  val localDestinatons = destinations.take(5)

  // joins

  case class FlightMetadata(count: BigInt, randomData: BigInt)

  val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
    .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
    .as[FlightMetadata]

  val flights2 = flights
    .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))

  flights2.selectExpr("_1.DEST_COUNTRY_NAME")
  flights2.take(2)

  // val flights2 = flights.join(flightsMeta, Seq("count"))
  // val flights2 = flights.join(flightsMeta.toDF(), Seq("count"))

  // grouping and aggregations
  flights.groupBy("DEST_COUNTRY_NAME").count()
  flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()

  def grpSum(countryName: String, values: Iterator[Flight]) = {
    values.dropWhile(_.count < 5).map(x => (countryName, x))
  }

  flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

  def grpSum2(f: Flight): Integer = {
    1
  }
  flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5)

  def sum2(left: Flight, right: Flight) = {
    Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
  }

  flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r))
    .take(5)

  flights.groupBy("DEST_COUNTRY_NAME").count().explain




}
