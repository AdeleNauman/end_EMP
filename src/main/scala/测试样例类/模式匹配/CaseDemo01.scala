package 测试样例类.模式匹配

import scala.util.Random

object CaseDemo01 extends App {
  val arr = Array("hadoop", "zookeeper", "spark")

  val name = arr(Random.nextInt(arr.length))
  name match {
    case "hadoop" => println("大数据分布式存储和计算框架...")
    case "zookeeper" => println("大数据分布式协调服务框架...")
    case "spark" => println("大数据分布式内存计算框架...")
    case _ => println("我不认识你...")

  }
}
