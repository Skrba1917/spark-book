package parttwo.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession
    .builder()
    .appName("ScalaBook")
    .config("spark.master", "local")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/retail-data/by-day/2010-12-01.csv")
  df.printSchema()
  df.createOrReplaceTempView("dfTable")


  // 3 TYPES OF COMPLEX TYPES: STRUCTS, ARRAYS AND MAPS

  // STRUCTS, we can think of them as dataframes within dataframes
  // we can type struct before the parenthesis
  df.selectExpr("(Description, InvoiceNo) as complex", "*")

  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")

  complexDF.select("complex.Description")
  complexDF.select(col("complex").getField("Description"))

  complexDF.select("complex.*")

  // ARRAYS
  // we split description column into array using split and delimiter
  df.select(split(col("Description"), " ")).show(2)


  df.select(split(col("Description"), " "))
    // returns the first position in array
    .selectExpr("array_col[0]").show(2)

  // find the length
  df.select(size(split(col("Description"), " "))).show(2)

  // we can see if the array contains some value, returns true or false
  df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

  // explode function takes a column that consists of arrays and creates one row
  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "exploded").show(2)

  // MAPS
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .show(2)

  // we can query maps based on the key, if key is missing it returns null
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")
  ).selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

  // we can explode map types which will turn them into columns
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("explode(complex_map)").show(2)






}
