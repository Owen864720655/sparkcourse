package example
import org.apache.spark.mllib.clustering.{ LDA, DistributedLDAModel }
import org.apache.spark.mllib.linalg.Vectors
import example.ExampleUtils._
import scala.io.Source
import java.io.File
import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import org.apache.spark.mllib.feature.HashingTF

/**
 * @author xiafan
 */
object LDAExample {

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

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext()
    // Load and parse the data
    val data = sc.textFile("data/sparkDocuments.txt")
    val tf = new HashingTF(100000)
    val parsedData = data.map(s => tf.transform(s.trim.split(' ')))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // 使用LDA训练3个话题
    val topicNum = 5
    val ldaModel = new LDA().setK(topicNum).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

    //查看每个topic中单词的分布
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, topicNum)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }

    // Save and load model.
    ldaModel.save(sc, "myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "myLDAModel")
  }
}