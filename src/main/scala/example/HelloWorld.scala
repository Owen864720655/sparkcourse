package example

/**
 * @author xiafan
 */
object HelloWorld {
  def main(args: Array[String]): Unit =
    {
      val hello = "Hello"
      val world = "scala"
      var message = String.format("%s %s!!!", hello, world)
      println(message);
      println(hello * 4);
    }
}