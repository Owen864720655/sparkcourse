package example.sparksql

import scala.reflect.runtime.universe

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import example.ExampleUtils.getSparkContext

/**
 * @author xiafan
 */

//http://doc.okbase.net/u014388509/archive/119763.html
//http://blog.csdn.net/oopsoom/article/details/42064075
object dfexample {
  case class Customer(custID: Int,
    name: String,
    address: String,
    nationKey: Int,
    phone: String,
    acctbal: Double,
    mktsegment: String,
    comment: String)
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext()
    // sc is an existing SparkContext. 
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame. 

    import sqlContext.implicits._

    // Create an RDD of Customer objects and register it as a table. 
    val customers = sc.textFile("/user/xiafan/dataset/spark/sql/dataset/customer.tbl").
      map(_.split("[|]")).
      map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt,
        p(4), p(5).trim.toDouble, p(6), p(7))).toDF()
    customers.registerTempTable("customers")

    customers.select("name").show()
    customers.select(customers("name"), customers("acctbal") + 1).show()
    customers.groupBy("mktsegment").count().show()
    customers.groupBy("mktsegment").sum("acctbal").show()
    customers.groupBy("mktsegment").max("acctbal").show()
    customers.filter("BUILDING=='BUILD'").show()
    sqlContext.sql("select * from customers where mktsegment = 'BUILDING'").show()

    val schema = StructType(
      StructField("nationKey", LongType, false) ::
        StructField("name", StringType, false) ::
        StructField("regionKey", LongType, false) ::
        StructField("comment", StringType, false) :: Nil)

    val nations = sc.textFile("/user/xiafan/dataset/spark/sql/dataset/nation.tbl")
    val rowRDD = nations.map(_.split("\\|")).
      map(p => Row(p(0).toLong, p(1), p(2).toLong, p(3).trim))
    val nationDf = sqlContext.createDataFrame(rowRDD, schema)
    nationDf.select("name")
    nationDf.filter("nationKey=1")
    nationDf.join(customers, nationDf("nationKey") === customers("nationKey"), "inner")
    nationDf.show(10)
  }
}