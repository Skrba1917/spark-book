package part3

import org.apache.spark.sql.SparkSession

object BroadcastVariables extends App {

  val spark = SparkSession.builder()
    .appName("BVariables")
    .config("spark.master", "local")
    .getOrCreate()

  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val words = spark.sparkContext.parallelize(myCollection, 2)

  val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
    "Big" -> -300, "Simple" -> 100)

  val suppBroadcast = spark.sparkContext.broadcast(supplementalData)

  suppBroadcast.value

  words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
    .sortBy(wordPair => wordPair._2)
    .collect()

}
