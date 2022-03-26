package 测试样例类.样例类

object OptionDemo {
  def main(args: Array[String]): Unit = {
    val map = Map("a"->1,"b"->"")
    val va = map.get("b") match {
      case Some(i) => i
      case None => 0
    }
    println(va)
    //更好的办法
    val va2 = map.getOrElse("c", 0)
    println(va2)
  }
}
