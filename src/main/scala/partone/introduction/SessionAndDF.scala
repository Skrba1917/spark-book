package introduction

import org.apache.spark.sql.SparkSession


object SessionAndDF extends App {

  val spark = SparkSession.builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val myRange = spark.range(1000).toDF("number").show()

  //val divisBy2 = myRange.where($"number % 2 = 0")
  //divisBy2.count()


}
