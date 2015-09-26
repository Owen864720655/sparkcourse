package example;
/**
 * 使用的数据是TPCH的DBGEN生成的测试数据，这里使用
 * CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
 * N_NAME       CHAR(25) NOT NULL,
 * N_REGIONKEY  INTEGER NOT NULL,
 * N_COMMENT    VARCHAR(152));
 *
 * CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
 * C_NAME        VARCHAR(25) NOT NULL,
 * C_ADDRESS     VARCHAR(40) NOT NULL,
 * C_NATIONKEY   INTEGER NOT NULL,
 * C_PHONE       CHAR(15) NOT NULL,
 * C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
 * C_MKTSEGMENT  CHAR(10) NOT NULL,
 * C_COMMENT     VARCHAR(117) NOT NULL);
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object PopWords {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val customer = sc.textFile("/Users/xiafan/Downloads/tpch_2_17_0/dbgen/customer.tbl")
    val comment = customer.map(rec => rec.split("[|]")(7))
    val wordCounts1 = comment.flatMap(p => p.split(" "))
      .map(p => (p, 1))
      .reduceByKey((a, b) => a + b)
    wordCounts1.map(rec => (rec._2, rec._1)).top(10)
  }

  def getSparkContext(): SparkContext = {
    //初始化SparkContext
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    return sc
  }

  /**
   * 这是最不推荐的方法，它导致每次任务分发时，nationMap都需要被传输一次
   */
  def computeByClosure(args: Array[String]): Unit = {
    val sc = getSparkContext()
    //读取数据文件
    val customer = sc.textFile(args(1))
    val nation = new Array[String](1)
    val nationMap = Map[Int, String]()
    val nationKeyCounts = customer.map(rec => (rec.split("[|]")(3).toInt, 1)).reduceByKey((x, y) => x + y)
    val nationNameCounts = nationKeyCounts.map(x => (nationMap.getOrElse(x._1, "unknown"), x._2))
    val data = nationNameCounts.map(x => (x._2, x._1))

    //这里top是根据key进行排序的，因此需要先将nationNameCounts的key,value对换一下
    val orderedNationNameCounts = nationNameCounts.map(x => (x._2, x._1)).sortByKey()
    orderedNationNameCounts.saveAsTextFile(args(3))
    /*
    //如果只想看前10条记录
    val orderedNationNameCounts = nationNameCounts.map(x => (x._2, x._1)).top(10)
    println(orderedNationNameCounts.mkString(","))
    */
  }

  /**
   * 这是最推荐的方法，采用broadcast方式，nation表在每台机器上面只会被拷贝一次
   */
  def computeByBroadcast(args: Array[String]): Unit = {
    val sc = getSparkContext()
    //读取数据文件
    val customer = sc.textFile(args(1))
    //load the nation files locally
    //val nation = sc.textFile("/Users/xiafan/Downloads/tpch_2_17_0/dbgen/nation.tbl")
    val nation = new Array[String](1)
    val nationMap = Map[Int, String]()
    val nationMapBC = sc.broadcast(nationMap)
    val nationKeyCounts = customer.map(rec => (rec.split("[|]")(3).toInt, 1)).reduceByKey((x, y) => x + y)
    val nationNameCounts = nationKeyCounts.map(x => (nationMapBC.value.getOrElse(x._1, "unknown"), x._2))
    //这里top是根据key进行排序的，因此需要先将nationNameCounts的key,value对换一下
    val orderedNationNameCounts = nationNameCounts.map(x => (x._2, x._1)).sortByKey()

    //存储
    orderedNationNameCounts.saveAsTextFile(args(3))
  }

  /**
   * 这个方法在nationMap非常大时采用
   */
  def computeByJoin(args: Array[String]): Unit = {
    val sc = getSparkContext()
    //读取数据文件
    val customer = sc.textFile(args(1))
    val nation = sc.textFile(args(2))

    val nationTable = nation.map(line => {
      val fields = line.split("[|]")
      (fields(0).toInt, fields(1))
    })
    val nationKeyCounts = customer.map(rec => (rec.split("[|]")(3).toInt, 1)).reduceByKey((x, y) => x + y)
    //joinTable里面每条记录为(nationKey, (count, nation_name))
    val joinTable = nationKeyCounts.join(nationTable)
    val orderedNationNameCounts = joinTable.map(rec => rec._2).sortByKey()

    //存储
    orderedNationNameCounts.saveAsTextFile(args(3))
  }
}