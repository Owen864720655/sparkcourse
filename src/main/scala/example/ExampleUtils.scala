package example
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author xiafan
 */
object ExampleUtils {
  def getSparkContext(appName: String = "test"): SparkContext = {
    //初始化SparkContext
    //val conf = new SparkConf()
    val sc = new SparkContext("local", "test", "")
    return sc
  }
}