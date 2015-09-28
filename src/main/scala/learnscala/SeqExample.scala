package learnscala
import scala.collection.mutable.ArrayBuffer;
/**
 * @author xiafan
 */
object SeqExample {
  def varLenArray(): Unit = {
    //实现一个可变的数组（数组缓冲），以下两种方式等价
    val aMutableArr = ArrayBuffer[Int]();
    val bMutableArr2 = new ArrayBuffer[Int];
    //用+=在尾端添加元素
    aMutableArr += 1;
    //在尾端添加多个元素
    aMutableArr += (2, 3, 4)
    //使用++=操作符追加任何集合
    aMutableArr ++= Array(8, 9, 10)
    //移除最后7个元素
    aMutableArr.trimStart(1)
    aMutableArr.update(1, 1)
    aMutableArr.trimEnd(3)
    for (elem <- aMutableArr) {
      print(elem + " , ")
    }
  }

  def fixLenArray(): Unit = {
    //两种定长数组的定义方式
    var aArr = Array(1, 2, 3, 4)
    var bArr = new Array[Int](4)
    //使用for语句遍历数组，使用yield返回新数组的元素，返回的是ArrayBuffer类型
    var result = for (ele <- aArr) yield ele * 2
    result.copyToArray(bArr)
    //数组遍历方式之foreach
    bArr.foreach(println)
    //通过下标进行遍历
    for (i <- 0 until bArr.length) {
      println(bArr(i))
    }
  }

  def listExample(): Unit = {
    val listOne = List(1, 2)
    val listTwo = List(3, 4)
    //两个List拼接,[1,2,3,4]
    val listThree = listOne ::: listTwo
    //一个List拼接一个元素,得到[5,1,2,3,4]
    val listFour = 5 :: listThree
    val listConcat = 1 :: 2 :: 3 :: 4 :: Nil
    println(listFour.slice(0, 3))
  }

  def mapExample(): Unit = {
    //默认为immutable HashMap
    var fixMap = Map("China" -> 1, "UK" -> 2, "USA" -> 3)
    fixMap = fixMap + ("Canada" -> 4)
    println(fixMap("China"))
    println(fixMap.getOrElse("Japan", 0))
    val mutableMap = scala.collection.mutable.Map[String, Int]()
    for ((k, v) <- fixMap) {
      mutableMap(k) = v
    }
  }

  def tupleExample(): Unit = {
    //tuple主要是能够自动识别变量类型，自己查看类型
    val tuple = (1, 2.0, "Spark", "Scala")
    //> tuple  : (Int, Double, String, String) = (1, 2.0, Spark, Scala)
    //遍历时从1开始,可以加占位符序列进行访问
    val one = tuple._1
    //>res4: String = Spark
    //可以把tuple给一变量，如果只需一部分，其他的用占位符代替
    val (first, second, third, fourth) = tuple //> first  : Int = 1
    //first: Int = 1
    //second: Double = 2.0
    //third: String = Spark
    //fourth: String = Scala
    val (a, b, _, _) = tuple 
    //> a  : Int = 1
    //| b  : Double = 2.0
  }
  def main(args: Array[String]): Unit = {
    listExample()
  }

}