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
    println(listFour)
    println(listConcat)
  }

  def mapExample(): Unit = {
    //默认为immutable HashMap
    val fixMap = Map("China" -> 1, "UK" -> 2, "USA" -> 3)
    
  }

  def main(args: Array[String]): Unit = {
    listExample()
  }

}