package streaming.flume

import org.apache.spark.{SparkConf, SparkContext}

object flumenew {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)
    
  }
}
