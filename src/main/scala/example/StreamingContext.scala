package example

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.wltea.analyzer.core.IKSegmenter
import java.io.StringReader
import org.wltea.analyzer.core.Lexeme
import scala.collection.mutable.ArrayBuffer

/**
 * @author xiafan
 */
object StreamingContext {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: HdfsWordCount <master> <directory>")
      System.exit(1)
    }

    // Create the context
    val ssc = new StreamingContext(args(0), "HdfsWordCount", Seconds(2),
      System.getenv("SPARK_HOME")) // StreamingContext.jarOfClass(this.getClass)

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(1))
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
          words
        })
      }).flatMap { x => x }

    val sc = ExampleUtils.getSparkContext()
    val stopWords = sc.textFile("").map(x => (x, null))

    val filteredRdd = words.transform(rdd => rdd.map(x => (x, null)).join(stopWords).map(x => x._1))

    val wordCounts = filteredRdd.map(x => (x, 1)).reduceByKey(_ + _)
    val sortedWords = wordCounts.map(x => (x._2, x._1)).transform(rdd => rdd.sortByKey())
    sortedWords.saveAsHadoopFiles("popwords", "txt")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}