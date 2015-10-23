package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author xiafan
 */
object SparkInteractiveExapmle {
  def main(args: Array[String]): Unit = {

  }

  class ClosureClass(val message: String) extends Serializable {
    def transform(line: String): String = {
      return line + this.message
    }
  }

  def planRDDOp(): Unit = {
    val sc = new SparkContext(new SparkConf())
    val logs = sc.textFile("file:/Users/xiafan/Documents/dataset/sparklecture/interactive/appfirewall.log")

    logs.count()

    logs.first()
    logs.take(10).foreach(println)

    //过滤
    val eclipseLog = logs.filter(line => line.contains("eclipse"))
    eclipseLog.take(10)
    val javaLog = logs.filter(line => line.contains("java"))
    javaLog.take(10)
    //一到多
    eclipseLog.flatMap(line => line.split(":")).take(10).foreach(println)
    //一到一
    eclipseLog.map(line => line.split(":")(3)).take(10).foreach(println)
    //对每个partition执行map函数
    eclipseLog.mapPartitionsWithIndex((idx, lines) => lines.map(x => x.split(":")(2))).take(10).foreach(println)

    //闭包验证
    val func = new ClosureClass("this is toy example")
    eclipseLog.map(line => func.transform(line)).take(10).foreach(println)

    //集合操作
    val unionLog = eclipseLog.union(javaLog)
    unionLog.count()

    val optPrograms = logs.map(line => {
      val fields = line.split(":")
      if (fields.length >= 4)
        Some(fields(3).trim())
      else
        None
    })

    val programs = optPrograms.filter(rec => !rec.isEmpty).map(f => f.get)

    programs.distinct().take(10)
    val selProgs = sc.parallelize(List("eclipse", "chromedriver", "kuaipan"))
    programs.intersection(selProgs).take(10)

    val progCount = programs.map(x => (x, 1))
    progCount.groupByKey().take(1)
    progCount.reduceByKey(_ + _).take(10)
    progCount.cogroup(selProgs.map(x => (x, 0))).take(1)
    progCount.join(selProgs.map(x => (x, 0))).take(1)

    val progOcurrs = progCount.reduceByKey(_ + _)
    progOcurrs.map(rec => (rec._2, rec._1)).sortByKey(false).take(1)
    progCount.aggregateByKey(0)((value, rec) => value + 1, _ + _)

    progCount.combineByKey(value => value,
      (state: Int, value) => state + value,
      (state1: Int, state2: Int) => state1 + state2).take(5)
  }
}