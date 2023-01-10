package sparktoolset

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._

object MlAndAdvancedAnalytics extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/retail-data/by-day/*.csv")

  staticDataFrame.printSchema()

  val preppedDataFrame = staticDataFrame
    .na.fill(0)
    .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
    .coalesce(5)

  // DF transformations
  val trainDataFrame = preppedDataFrame
    .where("InvoiceDate < '2011-07-01'")
  val testDataFrame = preppedDataFrame
    .where("InvoiceDate >= '2011-07-01'")

  println("TrainDF count is: " + trainDataFrame.count())
  println("TestDF count is " + testDataFrame.count())

  // ML StringIndexer transformation, turns days of week into corresponding numerical values, we need OneHotEncoder
  // to fix that issue that comes because we implicitly state that Saturday(6) is greater than Monday(1) (by pure numerical values)
  // These Boolean flags state whether that day of the week is relevant day of the week
  val indexer = new StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")

  val encoder = new OneHotEncoder()
    .setInputCol("day_of_week_index")
    .setOutputCol("day_of_week_encoded")

  // each of these will result in a set of columns that we will "assemble" into a vector.
  // all ml algorithms in Spark take as input Vector type which must be a set of numerical values

  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
    .setOutputCol("features")

  // we will set this into a pipeline so that any data in the future that we need to transform
  // can go through exact same process

  val transformationPipeline = new Pipeline()
    .setStages(Array(indexer, encoder, vectorAssembler))

  // we need to fit transformers to this dataset
  val fittedPipeline = transformationPipeline.fit(trainDataFrame)

  // take fitted pipeline and use it to transform all of our data in consistent and repeatable way
  val transformedTraining = fittedPipeline.transform(trainDataFrame)

  // difference is significant with .cache()
  // transformedTraining.cache()

  val kmeans = new KMeans()
    .setK(20)
    .setSeed(1L)

  val kmModel = kmeans.fit(transformedTraining)

  // kmModel.computeCost(transformedTraining) --- lets us see the resulting data cost which is currently high

  val transformedTest = fittedPipeline.transform(testDataFrame)






}
