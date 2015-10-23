package example.sparksql
import example.ExampleUtils._

/**
 * @author xiafan
 */
class sqlexample {
  def createTables(): Unit = {
    val sc = getSparkContext()
    // sc is an existing SparkContext. 
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame. 
    import sqlContext.implicits._
    sqlContext.sql("SET hive.metastore.warehouse.dir=/home/xiafan/Documents/hive;")
    sqlContext.sql("CREATE EXTERNAL TABLE  CUSTOMER(C_CUSTKEY INT,C_NAME VARCHAR(25),C_ADDRESS VARCHAR(40),C_NATIONKEY INT,C_PHONE CHAR(15),C_ACCTBAL  DOUBLE ,C_MKTSEGMENT CHAR(10),C_COMMENT  VARCHAR(117))ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'STORED AS TEXTFILE LOCATION 'file:///home/xiafan/Documents/hive/customer'");
  }
}