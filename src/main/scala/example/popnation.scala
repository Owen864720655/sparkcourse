package example;

/**
 * spark使用scala脚本编程示例,这个利用用于统计每个国家的用户数量，并且按照用户数逆序排列
 *  使用的数据是TPCH的DBGEN生成的测试数据，这里使用
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
import ExampleUtils._

object PopNation {

  case class PopNationArgs(
    customerFile: String = "",
    nationFile: String = "",
    outputDir: String = "",
    method: Int = 0)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[PopNationArgs]("pop nation") {
      head("scopt", "3.x")
      opt[String]('c', "customerFile") required () action { (x, c) =>
        c.copy(customerFile = x)
      } text ("customerFile is the customerFile directory")
      opt[String]('n', "nationFile") optional () action { (x, c) =>
        c.copy(nationFile = x)
      } text ("nationFile is the nationFile directory")
      opt[String]('o', "ouput") required () action { (x, c) =>
        c.copy(outputDir = x)
      } text ("input is the input directory")
      opt[Int]('m', "method") action { (x, c) =>
        c.copy(method = x)
      } text ("实现方法:1 使用闭包,2 使用广播变量,3使用连接操作")
      help("help") text ("prints this usage text")
    }

    parser.parse(args, new PopNationArgs()) match {
      case Some(config) => {
        config.method match {
          case 1 => computeByClosure(config)
          case 2 => computeByBroadcast(config)
          case 3 => computeByJoin(config)
        }
      }
      case None =>
        parser.showUsage
    }
  }
  def computePopWords(args: PopNationArgs): Unit = {
    val sc = getSparkContext("computePopWords")
    val customer = sc.textFile(args.customerFile)
    val comment = customer.map(rec => rec.split("[|]")(7))
    val wordCounts1 = comment.flatMap(p => p.split(" "))
      .map(p => (p, 1))
      .reduceByKey((a, b) => a + b)
    wordCounts1.map(rec => (rec._2, rec._1)).top(10)
  }

  /**
   * 这是最不推荐的方法，它导致每次任务分发时，nationMap都需要被传输一次
   */
  def computeByClosure(args: PopNationArgs): Unit = {
    val sc = getSparkContext("computeByClosure")
    //读取数据文件
    val customer = sc.textFile(args.customerFile)
    val nationMap = sc.textFile(args.nationFile).collect().
      map { x => { val fields = x.split("[|]"); (fields(0).toInt, fields(1)) } }.toMap

    val nationKeyCounts = customer.map(rec => (rec.split("[|]")(3).toInt, 1)).reduceByKey((x, y) => x + y)
    val nationNameCounts = nationKeyCounts.map(x => (nationMap.getOrElse(x._1, "unknown"), x._2))
    val data = nationNameCounts.map(x => (x._2, x._1))

    //这里top是根据key进行排序的，因此需要先将nationNameCounts的key,value对换一下
    val orderedNationNameCounts = nationNameCounts.map(x => (x._2, x._1)).sortByKey()
    orderedNationNameCounts.saveAsTextFile(args.outputDir)
    /*
    //如果只想看前10条记录
    val orderedNationNameCounts = nationNameCounts.map(x => (x._2, x._1)).top(10)
    println(orderedNationNameCounts.mkString(","))
    */
  }

  /**
   * 这是最推荐的方法，采用broadcast方式，nation表在每台机器上面只会被拷贝一次
   */
  def computeByBroadcast(args: PopNationArgs): Unit = {
    val sc = getSparkContext()
    //读取数据文件
    val customer = sc.textFile(args.customerFile)
    val nationMap = sc.textFile(args.nationFile).collect().
      map { x => { val fields = x.split("[|]"); (fields(0).toInt, fields(1)) } }.toMap

    val nationMapBC = sc.broadcast(nationMap)
    val nationKeyCounts = customer.map(rec => (rec.split("[|]")(3).toInt, 1)).reduceByKey((x, y) => x + y)
    val nationNameCounts = nationKeyCounts.map(x => (nationMapBC.value.getOrElse(x._1, "unknown"), x._2))
    //这里top是根据key进行排序的，因此需要先将nationNameCounts的key,value对换一下
    val orderedNationNameCounts = nationNameCounts.map(x => (x._2, x._1)).sortByKey()

    //存储
    orderedNationNameCounts.saveAsTextFile(args.outputDir)
  }

  /**
   * 这个方法在nationMap非常大时采用
   */
  def computeByJoin(args: PopNationArgs): Unit = {
    val sc = getSparkContext()
    //读取数据文件
    val customer = sc.textFile(args.customerFile)
    val nation = sc.textFile(args.nationFile)

    val nationTable = nation.map(line => {
      val fields = line.split("[|]")
      (fields(0).toInt, fields(1))
    })
    val nationKeyCounts = customer.map(rec => (rec.split("[|]")(3).toInt, 1)).reduceByKey((x, y) => x + y)
    //joinTable里面每条记录为(nationKey, (count, nation_name))
    val joinTable = nationKeyCounts.join(nationTable)
    val orderedNationNameCounts = joinTable.map(rec => rec._2).sortByKey()

    //存储
    orderedNationNameCounts.saveAsTextFile(args.outputDir)
  }
}