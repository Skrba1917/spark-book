package parttwo.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JSON extends App {

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

  val jsonDF = spark.range(1).selectExpr(
    """
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

  jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey")).show(2)

  // turning StructType into JSON
  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")))

  // we can use from_json to parse and we should specify the schema and map of options
  val parseSchema = new StructType(Array(
    new StructField("InvoiceNo", StringType, true),
    new StructField("Description", StringType, true)))
  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)



}
