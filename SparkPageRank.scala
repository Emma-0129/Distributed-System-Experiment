package cs.author

import org.apache.spark.sql.SparkSession

/**
 *
 * @Author Emma
 * @Date 2022/6/18 8:57
 * @Version 1.0
 */
object SparkPageRank {
  def showWarning() {
    System.err.println(
      """WARNNING!
      """.stripMargin)
  }

  def main(args: Array[String]) {
    showWarning()

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SparkPageRank")
      .getOrCreate()

    val iters = 10
    val lines = spark.read.textFile("D:\\Emma\\src\\main\\scala\\cs\\author\\input.txt").rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    spark.stop()
  }
}
