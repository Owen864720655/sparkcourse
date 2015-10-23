package example

/**
 * @author xiafan
 */
object OptionParser {
  type OptionMap = Map[Symbol, Any]
  def parserOption(arglist: List[String]): OptionMap = {

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--max-size" :: value :: tail =>
          nextOption(map ++ Map('maxsize -> value.toInt), tail)
        case "--min-size" :: value :: tail =>
          nextOption(map ++ Map('minsize -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil => nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail =>
          println("Unknown option " + option)
          System.exit(1)
          map
      }
    }
    val options = nextOption(Map(), arglist)
    println(options)
    options
  }
}