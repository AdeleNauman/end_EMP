package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark1 {
  def main(args: Array[String]): Unit = {

    /** 配置环境变量*/
    //1、初始化spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    //2、初始化sparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    /** 配置监听的目标端口*/
    //3、通过监控端口创建DStream 读进来的数据为一行一行的
    val lineStreams = ssc.socketTextStream("192.168.43.133", 9999)
    /** 对数据进行扁平化处理并进行数据切片*/
    //4、将每一行的数据进行切分 形成一个一个的单词
    val wordStreams = lineStreams.flatMap(_.split(" "))
    /** 将单词遍历出来*/
    //5、将单词映射为元组
    val wordandoneStreams = wordStreams.map(
      ((_, 1))
    )
    //6、将相同的单词错统计
    val wordbk = wordandoneStreams.reduceByKey(_ + _)
    //打印
    wordbk.print()//print方法会输出时间戳
    //启动sparkstreamingcontext
    ssc.start()
    ssc.awaitTermination()


  }
}
