package example

import java.io.File
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.feature.HashingTF
import example.ExampleUtils._
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.Tokenizer

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
    val files: Array[File] = dir.listFiles()
    for (file <- files) {
      if (!file.isDirectory()) {
        var content = ""
        Source.fromFile(file).foreach { x => content = content + x }
        contents += content
      }
    }

    contents = contents.map(p => p.replace("\n", " "))
    val writer = new FileWriter(args(1))
    contents.foreach { x =>
      {
        writer.write(x)
        writer.write("\n")
      }
    }
    writer.close()
  }

  def train(): Unit = {
    val sc = getSparkContext()
    // Load and parse the data
    val data = sc.textFile("data/sparkDocuments.txt")
    val tf = new HashingTF(1000000)
    val tokenizer = new Tokenizer()
    val parsedData = data.map(s => tf.transform(s.trim.split(" ")))
    val mapping = data.flatMap(s => s.trim.split(" ").map(word => (tf.indexOf(word), word))).distinct()

    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // 使用LDA训练3个话题
    val topicNum = 5
    val ldaModel = new LDA().setK(topicNum).setMaxIterations(20).run(corpus)

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
    for (words <- ldaModel.describeTopics(10)) {
      words._1.zip(words._2).foreach(x => topics += ((x._1, (topicIdx, x._2))))
      //      topics+= (topicIdx, ())
     // println(words._1.zip(words._2).mkString(","))
      topicIdx+=1
    }

    val topicsRDD = sc.parallelize(topics,1)
    val ret = mapping.join(topicsRDD)
    //Array[(Int, (String, (Int, Double)))] = Array((1504,(//,(1,0.016686350599884367)))
    ret.collect().map(x => (x._2._2._1, (x._2._2._2, x._2._1,x._1))).sorted.foreach(println)
    // Save and load model.
    ldaModel.save(sc, "myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "myLDAModel")

  }
  def main(args: Array[String]): Unit = {
    mergeFiles(args)
  }
}