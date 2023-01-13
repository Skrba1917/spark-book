package part2.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserDefinedFunctions extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()


  val udfExampleDF = spark.range(5).toDF("num")
  def power3(number: Double): Double = number * number * number
  power3(2.0)

  // we need to register the function to make it available as DataFrame function
  val power3udf = udf(power3(_:Double):Double)
  udfExampleDF.select(power3udf(col("num"))).show()

  // we can also register this function as spark sql function
  spark.udf.register("power3", power3(_: Double): Double)
  udfExampleDF.selectExpr("power3(num)").show(2)
}
