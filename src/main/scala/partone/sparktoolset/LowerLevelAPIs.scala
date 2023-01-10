package sparktoolset

import org.apache.spark.sql.SparkSession

object LowerLevelAPIs extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._


  // Resilient Distributed Datasets
  // One thing we can use RDDs for is to parallelize raw data that we have stored in memory
  // on the driver machine
  spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()


}
