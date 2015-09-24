package learnscala

/**
 * @author xiafan
 */
object FunctionExample {

  /**
   * num:Double类型，指数操作的底
   * power:Double类型，默认值为2.0，指数操作中的指数值
   */
  def power(num: Double, power: Double = 2.0): Double = {
    return Math.pow(num, power)
  }

  def log(level: Int, args: String*): Unit = {
    println("level:%d, message:%s".format(level, args.mkString(",")))
  }

  def closureFunc(): Unit = {
    val people = List("Jim Gray", "Micheal Stonebrake",
      "Hector Garcia-Molina")
    def greeting(): Unit = {
      people.foreach(p => println("hello " + p))
    }
    greeting()
  }

  def main(args: Array[String]): Unit = {
    println(power(2))
    println(power(num = 2, power = 2))
    log(0, "hello", "scala")

    var powerFunc = power _
    println(powerFunc(2, 2))
    powerFunc = (num: Double, power: Double) => {
      Math.pow(num, power)
    }
  }
}