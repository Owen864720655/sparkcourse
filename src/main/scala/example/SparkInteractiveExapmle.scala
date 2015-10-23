package example

import java.io.StringReader

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.core.Lexeme


/**
 * 统计每个时间点的热门搜索词，这里使用的是sogou的搜索日志
 *
 * @author xiafan
 */
object StreamingContext {
  case class WordCounts(word: String, count: Int)

  def main(args: Array[String]) {
    val opt: Map[Symbol, Any] = Map[Symbol, Any]()
    def usage(): Unit = System.err.println("Usage: HdfsWordCount --input inputdir --stopwords stopfile")
    def nextOption(map: Map[Symbol, Any], list: List[String]): Map[Symbol, Any] = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value), tail)
        case "--stopwords" :: value :: tail =>
          nextOption(map ++ Map('stopwords -> value), tail)
        //case string :: opt2 :: tail if isSwitch(opt2) =>
        //  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail =>
          println("Unknown option " + option)
          usage()
          System.exit(1)
          map
      }
    }
    val options = nextOption(Map(), args.toList)
    if (options.size != 2) {
      usage()
      System.exit(1)
    }
    println("options are " + options.toString())
    // Create the context
    val conf = new SparkConf()
    conf.setAppName("hdfswordcount")
    val ssc = new StreamingContext(conf, Seconds(10))

    // 创建一个FileInputDStream监控给定的目录
    val lines = ssc.textFileStream(options('input).toString())
    //统计新文件中单词的次数，这里首先对每个查询中的查询词进行分词
    val words = lines.mapPartitions(
      lines => {
        val segmenter = new IKSegmenter(new StringReader(""), true)
        lines.map(line => {
          val words = new ArrayBuffer[String]()
          segmenter.reset(new StringReader(line))
          var token: Lexeme = null
          var hasNext = true
          while (hasNext) {
            token = segmenter.next()
            if (token != null) {
              // println()
              words += token.getLexemeText()
            } else {
              hasNext = false
            }
          }
          words.toArray
        })
      }).flatMap { x => x.map(word => word) }

    //加载通用词，有兴趣的同学可以考虑使用broadcast变量来实现一下
    val sc = ssc.sparkContext
    val stopWords = sc.textFile(options('stopwords).toString()).map(x => (x, null))
    //这里将dstream和rdd进行操作
    val filteredRdd = words.transform(rdd => rdd.map(x => (x, null)).join(stopWords).map(x => x._1))

    //周期性计算过去60秒内单词搜索次数
    val peroidCounts = filteredRdd.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(20), Seconds(10))
    val sqlContext = new HiveContext(ssc.sparkContext)
    import sqlContext.implicits._
    peroidCounts.foreachRDD { rdd =>
      // Convert RDD to DataFrame and register it as a SQL table
      val wordCountsDataFrame = rdd.map(x => WordCounts(x._1, x._2)).toDF()
      wordCountsDataFrame.registerTempTable("word_counts")
    }
    // Start the JDBC server
    HiveThriftServer2.startWithContext(sqlContext)

    //统计每个rdd里面的最热词
    val wordCounts = filteredRdd.map(x => (x, 1)).reduceByKey(_ + _)
    val sortedWords = wordCounts.map(x => (x._2, x._1)).transform(rdd => rdd.sortByKey())
    sortedWords.print(10)
    peroidCounts.saveAsTextFiles("peroid", "pop")

    ssc.checkpoint("hdfs://10.11.1.42/user/xiafan/check")
    ssc.start()
    ssc.awaitTermination()
  }
}