package part2.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateAndTimestamps extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
  dateDF.createOrReplaceTempView("dateTable")

  dateDF.show()
  dateDF.printSchema()

  // add and subtract 5 days from today
  dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5))
    .show(5)


  // difference between dates, datediff returns number between 2 dates
  // there is also months_between that returns months
  dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today")
    )).show(1)

  dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end")
  ).select(months_between(col("start"), col("end")))
    .show(1)

  // to_date functions allows us to convert a string to date and we can specify the format
  spark.range(5).withColumn("date", lit("2017-01-01"))
    .select(to_date(col("date"))).show(1)

  // spark will not throw error if he can't parse the date, instead he will just return null.

  // returns null in the first column, and it will return December 11th instead of November 12th
  // which is wrong. Spark can't know whether the days are mixed up or that specific row is incorrect
  // dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)

  // we need to fix this pipeline below
  val dateFormat = "yyyy-dd-MM"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2")
  )
  cleanDateDF.createOrReplaceTempView("dateTable2")

  // example of to_timestamp and it always requires format to be specified
  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show(2)

  // comparing
  cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

}
