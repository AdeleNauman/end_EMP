package app广告.不知道是啥

import scala.io.StdIn

object a {

  def main(args: Array[String]): Unit = {

    println("开始投币")
    var a = StdIn.readInt()
    var 吕 = 0
    var 桑 = 0

    for (i <- 1 to a) {
      val sss = scala.util.Random.nextInt(2)
      println(sss)
      if (sss == 1) {
        吕 = 吕 + 1
        println("正面次数：" + 吕)
      } else {
        桑 = 桑 + 1
        println("反面次数：" + 桑)
      }

      //        def glob(sss: Any): Any = sss match {
      //          case 1 =>
      //            println("正面的次数")
      //            吕 = 吕 + 1
      //            println(吕)
      //          case 0 =>
      //            println("反面的次数")
      //            var b = StdIn.readInt()
      //            println(桑)
      //
      //        }
    }
  }
}
