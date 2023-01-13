package part2.basicstructuredoperations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object DataFrames extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.format("json")
    .load("src/main/resources/data/flight-data/json/2015-summary.json")
  df.createOrReplaceTempView("dfTable")

  val myManualSchema = new StructType(Array(
    StructField("some", StringType, true),
    StructField("col", StringType, true),
    StructField("names", LongType, false,
  )))
  val myRows = Seq(Row("Hello", null, 1L))

  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf = spark.createDataFrame(myRDD, myManualSchema)

  //myDf.show()

  df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

  // There are many different ways to do select, expr, selectExpr etc.
  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

  df.selectExpr(
    "*",
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry"
  ).show(2)

  df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

  // CONVERTING TO SPARK TYPES (LITERALS)
  df.select(expr("*"), lit(1).alias("One")).show(2)

  // ADDING COLUMNS (boolean column based on expression result) otherwise it would just be a value next to column name
  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

  // RENAMING COLUMNS
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

  // USING BACKTICK FOR SPECIAL RESERVED WORDS AND BLANK SPACES, WE DONT NEED IT HERE
  val dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME")
  )

  // HERE WE NEED IT BECAUSE WE ARE REFERENCING THE COLUMN
  dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`"
  ).show(2)

  // REMOVING COLUMNS
  df.drop("ORIGIN_COUNTRY_NAME").columns

  // CHANGING COLUMN TYPE
  df.withColumn("count2", col("count").cast("long"))

  // FILTERING ROWS
  // these 2 are the same
  df.filter(col("count") < 2).show(2)
  df.where("count < 2").show(2)

  // CHAINED FILTERING
  df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)

  // GETTING UNIQUE ROWS, distinct deduplicates any rows that are in DataFrame
  df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

  // RANDOM SAMPLES to sample data from DataFrame
  val seed = 5
  val withReplacement = false
  val fraction = 0.5
  df.sample(withReplacement, fraction, seed).count()

  // RANDOM SPLITS
  val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
  dataFrames(0).count() > dataFrames(1).count() // FALSE

  // CONCATENATING AND APPENDING ROWS (UNION), dfs are immutable so to append to it
  // we need to union the original df along with the new df
  // dfs need to have exact same schema and number of columns or the union will fail

  val schema = df.schema
  val newRows = Seq(
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", " Other Country 3", 1L)
  )
  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, schema)

  df.union(newDF)
    .where("count = 1")
    .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
    .show()

  // We use =!= so that we dont just compare unevaluated column expression to a string but instead
  // to the evaluated one

  // SORTING ROWS
  df.sort("count").show(5)
  df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
  df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
  // we can use asc or desc for specific sort direction

  // we can also use sortWithinPartitions
  spark.read.format("json").load("src/main/resources/data/flight-data/json/*-summary.json")
    .sortWithinPartitions("count")

  // LIMIT, we can use limit method to see for example top 10 of some data frame
  df.limit(5).show()
  df.orderBy(expr("count desc")).limit(6).show()

  // REPARTITION AND COALESCE
  df.rdd.getNumPartitions // 1
  df.repartition(5)
  // if we know that we are going to be filtering a certain column often, we can repartition based on that column
  df.repartition($"DEST_COUNTRY_NAME")
  // we can also specify the number of partitions
  df.repartition(5, $"DEST_COUNTRY_NAME")


  // COLLECTING ROWS TO THE DRIVER
  val collectDf= df.limit(10)
  collectDf.take(5) // take works with an Integer count
  collectDf.show() // this prints it out
  collectDf.show(5, false)
  collectDf.collect()

  // collects partitions to the driver as an iterator
  collectDf.toLocalIterator()







}
