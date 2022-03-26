package 测试样例类.模式匹配

import scala.util.Random

object CaseDemo02 {
  def main(args: Array[String]): Unit = {
    println(a)
  }

  def a {
    val arr = Array("hello", 1, 2.0, "CaseDemo")
    val v = arr(Random.nextInt(4))
    println(v)

    v match {

      case x: Int => println("Int " + x)

      case y: Double if (y >= 0) => println("Double " + y)

      case z: String => println("String " + z)

      case _ => throw new Exception("not match exception")

    }
  }

}
