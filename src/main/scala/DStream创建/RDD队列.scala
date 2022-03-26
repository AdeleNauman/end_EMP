package DStream创建

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.reflect.internal.util.TriState.False

object RDD队列 {

  def main(args: Array[String]): Unit = {

    //1、初始化Spark信息
    val rDDstreaming = new SparkConf().setMaster("local[*]").setAppName("RDDstreaming")
    //2、初始化StreamingContext    并设置处理的匹次时间
    val sc = new StreamingContext(rDDstreaming,Seconds(4))
    //3、创建RDD队列
    val queue = new mutable.Queue[RDD[Int]]

    //4、创建QueueInputDStream
    val inputStream = sc.queueStream(queue, oneAtATime = false)
    //5、处理队伍中rdd的数据
    //先进行转元组操作
    val mapedstream = inputStream.map((_,1))
    //将元组数据进行聚合
    val reducebk = mapedstream.reduceByKey(_+_)
    //6、打印结果
    reducebk.print()
    //7、启动任务
    sc.start()
    //8、循环向rdd队列中放入rdd
    for(i <- 1 to 5){
      // 累加数据到 创建的rdd中
      queue += sc.sparkContext.makeRDD(1 to 30,300)
      Thread.sleep(2000)
    }
    sc.awaitTermination()
  }
}
