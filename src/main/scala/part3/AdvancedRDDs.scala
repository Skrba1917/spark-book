package part3

import org.apache.spark.sql.SparkSession

object AdvancedRDDs extends App {

  val spark = SparkSession.builder()
    .appName("AdvancedRDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val words = spark.sparkContext.parallelize(myCollection, 2)

  // key-value basics (key-value rdds)
  words.map(word => (word.toLowerCase, 1))

  // keyBy
  val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

  // mapping over values
  keyword.mapValues(word => word.toUpperCase).collect() // we can also use flatMap same as in previous example of RDDs

  // extracting keys and values
  keyword.keys.collect()
  keyword.values.collect()

  // lookup
  keyword.lookup("s")

  // sampleByKey
  val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
    .collect()

  import scala.util.Random

  val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
  words.map(word => (word.toLowerCase.toSeq(0), word))
    .sampleByKey(true, sampleMap, 6L)
    .collect()

  // aggregations
  val chars = words.flatMap(word => word.toLowerCase.toSeq)
  val KVcharacters = chars.map(letter => (letter, 1))

  def maxFunc(left: Int, right: Int) = math.max(left, right)

  def addFunc(left: Int, right: Int) = left + right

  val nums = spark.sparkContext.parallelize(1 to 30, 5)

  // countByKey
  val timeout = 1000L //milliseconds
  val confidence = 0.95
  KVcharacters.countByKey()
  KVcharacters.countByKeyApprox(timeout, confidence)

  // groupByKey
  KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()

  // reduceByKey
  KVcharacters.reduceByKey(addFunc).collect()

  // aggregate
  nums.aggregate(0)(maxFunc, addFunc)

  val depth = 3
  nums.treeAggregate(0)(maxFunc,  addFunc, depth)

  // aggregateByKey
  KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()

  // combineByKey
  val valToCombiner = (value: Int) => List(value)
  val mergeValuesFunc = (vals: List[Int], valToAppend: Int) => valToAppend :: vals
  val mergeCombinerFunc = (vals1: List[Int], vals2: List[Int]) => vals1 ::: vals2
  // now we define these as function variables
  val outputPartitions = 6
  KVcharacters
    .combineByKey(
      valToCombiner,
      mergeValuesFunc,
      mergeCombinerFunc,
      outputPartitions)
    .collect()

  // foldByKey
  KVcharacters.foldByKey(0)(addFunc).collect()

  // CoGroups

  import scala.util.Random

//  val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
//  val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
//  val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
//  val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
//  charRDD.cogroup(charRDD2, charRDD3).take(5)

  // inner join (other joins work too, they follows the same format)
//  val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
//  val outputPartitions = 10
//  KVcharacters.join(keyedChars).count()
//  KVcharacters.join(keyedChars, outputPartitions).count()

  // zips
  val numRange = spark.sparkContext.parallelize(0 to 9, 2)
  words.zip(numRange).collect()

  // Controling Partitions
  // coalesce
  words.coalesce(1).getNumPartitions // 1

  // repartitions
  words.repartition(10) // gives us 10 partitions

  // custom partitioning
  val df = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("src/main/resources/data/retail-data/all/")
  val rdd = df.coalesce(10).rdd
  // hash partitioner

  import org.apache.spark.HashPartitioner

  rdd.map(r => r(6)).take(5).foreach(println)
  val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
  keyedRDD.partitionBy(new HashPartitioner(10)).take(10)


  import org.apache.spark.Partitioner

  class DomainPartitioner extends Partitioner {
    def numPartitions = 3

    def getPartition(key: Any): Int = {
      val customerId = key.asInstanceOf[Double].toInt
      if (customerId == 17850.0 || customerId == 12583.0) {
        return 0
      } else {
        return new java.util.Random().nextInt(2) + 1
      }
    }
  }

  keyedRDD
    .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
    .take(5)

  // custom serialization
  class SomeClass extends Serializable {
    var someValue = 0

    def setSomeValue(i: Int) = {
      someValue = i
      this
    }
  }

  spark.sparkContext.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))




}
