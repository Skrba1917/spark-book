package parttwo.differenttypesofdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Strings extends App {

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

  // capitalizes every word
  df.select(initcap(col("Description"))).show(2, false)

  // all to lower/upper case
  // .upper(col("Description")) or .lower

  // adding or removing spaces around strings
  // if lpad or rpad takes a number less than the length of the string, it will always remove values
  // from the right side of the string
  df.select(
    ltrim(lit("   HELLO    ")).as("ltrim"),
    rtrim(lit("   HELLO    ")).as("rtrim"),
    trim(lit("    HELLO    ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp")
  ).show(2)

  // REGULAR EXPRESSIONS
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  // the '|' signifies `OR` in regular expression syntax
  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description")
  ).show(2)

  // translate function
  df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
    .show(2)

  // pulling out first mentioned color
  val regexString1 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
  df.select(
    regexp_extract(col("Description"), regexString1, 1).alias("color_clean_two"),
    col("Description")
  ).show(2)

  // we can check for value's existence with contains method
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("Description").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(3, false)

  // dynamically creating numbers of columns
  val selectedColumns = simpleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*")
  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

}
