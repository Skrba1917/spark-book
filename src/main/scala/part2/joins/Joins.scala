package part2.joins

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  // inner joins

  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
  person.join(graduateProgram, joinExpression).show() // inner is the default join so we don't need to specify it

  // outer joins
  person.join(graduateProgram, joinExpression, "outer").show()

  // left outer join
  person.join(graduateProgram, joinExpression, "left_outer").show()

  // right outer join
  person.join(graduateProgram, joinExpression, "right_outer").show()

  // left semi joins
  graduateProgram.join(person, joinExpression, "left_semi")
  val gradProgram2 = graduateProgram.union(Seq((0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
  gradProgram2.createOrReplaceTempView("gradProgram2")

  // left anti joins
  graduateProgram.join(person, joinExpression, "left_anti").show()

  // cross joins
  graduateProgram.join(person, joinExpression, "cross").show()



}
