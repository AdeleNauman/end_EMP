package flume

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.net.InetSocketAddress

object flumeapi {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("")
    var sc = new StreamingContext(conf, Seconds(5))
    //链接主机端口
    val addres=Seq(new InetSocketAddress("192.168.43.133",18888))
    //选取链接方式
    val stream = FlumeUtils.createPollingStream(sc, addres, StorageLevel.MEMORY_AND_DISK_2)
    //对数进行处理转为array
    val lineDstream = stream.map(x => new String(x.event.getBody.array()))
    //对数据切片 并转为元组
    val wordAndone = lineDstream.flatMap(_.split(" ")).map((_, 1))
    //对数据进行聚合操作
    val reslut = wordAndone.reduceByKey(_ + _)
    reslut.print()
    //启动流式程序
    sc.start()
    sc.awaitTermination()
  }
}
