package 模拟

import org.apache.spark.{SparkConf, SparkContext}

object 数据倾斜 {
  def main(args: Array[String]): Unit = {

    val rdd = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(rdd)
    val value = sc.textFile("data/1433223.txt",3)


    value.collect().foreach(println)

  }

}
