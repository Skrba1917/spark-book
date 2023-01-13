package part2.joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsOnComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("JoinsOnComplexTypes")
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

  person.withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)"))
    .show()

  // handling duplicated columns

  // creating a problem dataset to illustrate this
  val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
  val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
  // now we have 2 graduate program columns and we will get an error because spark doesn't know to which to refer to
  person.join(gradProgramDupe, joinExpr).show()

  // problem arises here
  // person.join(gradProgramDupe, joinExpr).select("graduate_program").show()

  // we can solve it by doing the following:

  // 1 - different join expression
  person.join(gradProgramDupe, "graduate_program").select("graduate_program").show()

  // 2 - dropping the column after join
  person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
    .select("graduate_program").show()

  val joinExpr2 = person.col("graduate_program") === graduateProgram.col("id")
  person.join(graduateProgram, joinExpr2).drop(graduateProgram.col("id")).show()

}
