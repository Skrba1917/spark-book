package part3

import org.apache.avro.file.BZip2Codec
import org.apache.spark.sql.SparkSession

object RDD extends App {

  val spark = SparkSession.builder()
    .appName("RDD")
    .config("spark.master", "local")
    .getOrCreate()

  spark.range(500).rdd
  spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

  // creating from local collection
  val myCollection = "Spark the Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val words = spark.sparkContext.parallelize(myCollection, 2)
  words.setName("myWords")

  // from data sources
  spark.sparkContext.textFile("/some/path/withTextFiles")

  // TRANSFORMATIONS
  // distinct
  words.distinct().count()

  // filter
  def startsWithS(individual: String) = {
    individual.startsWith("S")
  }
  words.filter(word => startsWithS(word)).collect()

  // map
  val words2 = words.map(word => (word, word(0), word.startsWith("S")))
  words2.filter(record => record._3).take(5)

  // flatMap
  words.flatMap(word => word.toSeq).take(5)

  // sort
  words.sortBy(word => word.length() * -1).take(2)

  // random splits
  val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

  // ACTIONS
  // reduce
  spark.sparkContext.parallelize(1 to 20).reduce(_ + _) // 210

  def wordLengthReducer(leftWord: String, rightWord: String): String = {
    if (leftWord.length > rightWord.length)
      return leftWord
    else
      return rightWord
  }
  words.reduce(wordLengthReducer)

  // count
  words.count()

  // countApprox
  val confidence = 0.95
  val timeoutMiliseconds = 400
  words.countApprox(timeoutMiliseconds, confidence)

  // countApproxDistinct
  words.countApproxDistinct(0.05)
  words.countApproxDistinct(4, 10)

  // countByValue
  words.countByValue()

  // countByValueApprox
  words.countByValueApprox(1000, 0.95)

  // first
  words.first()

  // max & min
  spark.sparkContext.parallelize(1 to 20).max()
  spark.sparkContext.parallelize(1 to 20).min()

  // take

  words.take(5)
  words.takeOrdered(5)
  words.top(5)
  val withReplacement = true
  val numberToTake = 6
  val randomSeed = 100L
  words.takeSample(withReplacement, numberToTake, randomSeed)

  // Saving Files
  words.saveAsTextFile("file:/tmp/bookTitleCompressed") // we can set a compression codec (optional) - classOf[BZip2Codec]

  // SequenceFiles
  words.saveAsObjectFile("src/main/resources/data/tmp/my/sequenceFilePath")

  // Caching
  words.cache()
  words.getStorageLevel

  // checkpointing
  spark.sparkContext.setCheckpointDir("some/path/for/checkpointing")
  words.checkpoint()

  // Pipe RDDs to System Commands
  words.pipe("wc -l").collect()

  // mapPartitions
  words.mapPartitions(part => Iterator[Int](1)).sum() // 2

  def indexedFunc(partitionIndex: Int, withinPartIterator: Iterator[String]) = {
    withinPartIterator.toList.map(
      value => s"Partition: $partitionIndex => $value").iterator
  }

  words.mapPartitionsWithIndex(indexedFunc).collect()

  // foreachPartition
  words.foreachPartition { iter =>
    import java.io._
    import scala.util.Random
    val randomFileName = new Random().nextInt()
    val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
    while (iter.hasNext) {
      pw.write(iter.next())
    }
    pw.close()
  }

  // glom
  spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()




}
