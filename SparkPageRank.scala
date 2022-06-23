package cs.author

import org.apache.spark.sql.SparkSession

/**
 *
 * @Author wangyijie
 * @Date 2022/6/23 8:57
 * @Version 1.0
 */
object SparkPageRank {
  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
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
    val lines = spark.read.textFile("D:\\my\\src\\main\\scala\\cs\\author\\input.txt").rdd
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
