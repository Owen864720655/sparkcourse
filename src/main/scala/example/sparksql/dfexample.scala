package example.sparksql
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import example.PopWords

/**
 * @author xiafan
 */
case class Customer(custID: Int,
  name: String,
  address: String,
  nationKey: Int,
  phone: String,
  acctbal: Double,
  mktsegment: String,
  comment: String)
//http://doc.okbase.net/u014388509/archive/119763.html
  //http://blog.csdn.net/oopsoom/article/details/42064075
object dfexample {

  def main(args: Array[String]): Unit = {
    val sc = PopWords.getSparkContext()
    // sc is an existing SparkContext. 
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame. 
    import sqlContext.implicits._

    // Create an RDD of Customer objects and register it as a table. 
    val customers = sc.textFile("/home/xiafan/dataset/spark/sql/dataset/customer.tbl").
      map(_.split("[|]")).
      map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt,
        p(4), p(5).trim.toDouble, p(6), p(7))).toDF()
    customers.registerTempTable("customers")

    customers.select("name").show()
    customers.select(customers("name"), customers("acctbal") + 1).show()
    customers.groupBy("mktsegment").count().show()
    customers.groupBy("mktsegment").sum("acctbal").show()
    customers.groupBy("mktsegment").max("acctbal").show()
    customers.filter(customers("BUILDING") == "BUILD").show()
    sqlContext.sql("select * from customers where mktsegment = 'BUILDING'").show()
  }
}