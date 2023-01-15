package part3

import org.apache.spark.sql.SparkSession

object Accumulators extends App {

  val spark = SparkSession.builder()
    .appName("Accumulators")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String, count: BigInt)

  val flights = spark.read
    .parquet("/data/flight-data/parquet/2010-summary.parquet")
    .as[Flight]

  import org.apache.spark.util.LongAccumulator

  val accUnnamed = new LongAccumulator
  val acc = spark.sparkContext.register(accUnnamed)

  val accChina = new LongAccumulator
  val accChina2 = spark.sparkContext.longAccumulator("China")
  spark.sparkContext.register(accChina, "China")

  def accChinaFunc(flight_row: Flight) = {
    val destination = flight_row.DEST_COUNTRY_NAME
    val origin = flight_row.ORIGIN_COUNTRY_NAME
    if (destination == "China") {
      accChina.add(flight_row.count.toLong)
    }
    if (origin == "China") {
      accChina.add(flight_row.count.toLong)
    }
  }

  flights.foreach(flight_row => accChinaFunc(flight_row))

  accChina.value // 953

  // custom accumulators

  import scala.collection.mutable.ArrayBuffer
  import org.apache.spark.util.AccumulatorV2

  val arr = ArrayBuffer[BigInt]()

  class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
    private var num: BigInt = 0

    def reset(): Unit = {
      this.num = 0
    }

    def add(intValue: BigInt): Unit = {
      if (intValue % 2 == 0) {
        this.num += intValue
      }
    }

    def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = {
      this.num += other.value
    }

    def value(): BigInt = {
      this.num
    }

    def copy(): AccumulatorV2[BigInt, BigInt] = {
      new EvenAccumulator
    }

    def isZero(): Boolean = {
      this.num == 0
    }
  }

//  val acc = new EvenAccumulator
//  val newAcc = spark.sparkContext.register(acc, "evenAcc")
//  acc.value // 0
//  flights.foreach(flight_row => acc.add(flight_row.count))
//  acc.value // 31390


}
