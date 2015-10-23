package example

import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileWriter
import java.io.InputStreamReader
import java.io.StringReader
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.core.Lexeme
import example.ExampleUtils.getSparkContext

/**
 * @author xiafan
 */
object LDAExample {

  /**
   * args[0]:input data directory
   * args[1]: output merged file
   */
  def mergeFiles(args: Array[String]): Unit = {
    var contents = new ArrayBuffer[String]()
    val dir: File = new File(args(0))
    val writer = new FileWriter(args(1))
    mergeFile(dir, writer)
    writer.close()
  }

  def mergeFile(dir: File, writer: FileWriter): Unit = {
    println("visiting file:" + dir.toString)
    val files: Array[File] = dir.listFiles()
    for (file <- files) {
      if (!file.isDirectory()) {
        println(file.toString())
        val content = new StringBuffer()
        val in = new FileInputStream(file);
        val decoder = Charset.forName("UTF-8").newDecoder();
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        val reader = new BufferedReader(new InputStreamReader(in, decoder))
        var line: String = null
        do {
          line = reader.readLine()
          if (line != null) {
            content.append(line.replace("&nbsp", " "))
            content.append(" ")
          }
        } while (line != null);
        in.close()
        writer.write(content.toString())
        writer.write("\n")
      } else {
        mergeFile(file, writer)
      }
    }
  }

  def train(): Unit = {
    val sc = getSparkContext()
    // Load and parse the data
    var corpusFile = "/Users/xiafan/Downloads/corpus.txt"
    corpusFile = "/home/xiafan/dataset/spark/ml/corpus.txt"
    var stopwordsFile = "/Users/xiafan/Documents/dataset/sparklecture/stopwords.txt"
    stopwordsFile = "/home/xiafan/dataset/spark/ml/stopwords.txt"
    val data = sc.textFile(corpusFile)
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile)
      .collect().toSet)
    val tf = new HashingTF()
    // val tokenizer = new Tokenizer()
    // val parsedData = data.map(s => tf.transform(s.trim.split(" ")))

    var docWords = data.mapPartitions(lines => {
      val segmenter = new IKSegmenter(new StringReader(""), true)
      lines.map(line => {
        val words = new ArrayBuffer[String]()
        segmenter.reset(new StringReader(line))
        var token: Lexeme = null
        var hasNext = true
        while (hasNext) {
          token = segmenter.next()
          if (token != null) {
            val word = token.getLexemeText()
            if (word.length() > 1 && !stopwords.value.contains(word)) {
              words += word
            }
          } else {
            hasNext = false
          }
        }
        words
      })
    })
    //将单词构成的list转换成vector
    var parsedData = docWords.map(words =>
      tf.transform(words))

    val mapping = docWords.flatMap(words =>
      words.map(word => (tf.indexOf(word), word))).distinct()

    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // 使用LDA训练3个话题
    val topicNum = 8
    val ldaModel = new LDA().setMaxIterations(50).setK(topicNum).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

    //    //查看每个topic中单词的分布
    //    val topics = ldaModel.topicsMatrix
    //    for (topic <- Range(0, topicNum)) {
    //      print("Topic " + topic + ":")
    //      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
    //      println()
    //    }

    //以下代码只是为了显示好看
    var topics = new ArrayBuffer[(Int, (Int, Double))]()
    var topicIdx = 1
    for (words <- ldaModel.describeTopics(20)) {
      words._1.zip(words._2).foreach(x => topics += ((x._1, (topicIdx, x._2))))
      //      topics+= (topicIdx, ())
      println(words._1.zip(words._2).mkString(","))
      topicIdx += 1
    }

    val topicsRDD = sc.parallelize(topics, 1)
    val ret = mapping.join(topicsRDD)
    //Array[(Int, (String, (Int, Double)))] = Array((1504,(//,(1,0.016686350599884367)))
    ret.collect().map(x => (x._2._2._1, (x._2._2._2, x._2._1))).sorted.foreach(println)
    // Save and load model.
    ldaModel.save(sc, "myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "myLDAModel")

  }
  def main(args: Array[String]): Unit = {
    mergeFiles(args)
    // train()
  }
}