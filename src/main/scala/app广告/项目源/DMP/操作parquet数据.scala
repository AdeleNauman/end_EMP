package app广告.项目源.DMP

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.{SaveMode, SparkSession}
object 操作parquet数据 {
  //指定hdfs根节点
  private val hdfsRoot = "hdfs://master:8020"

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("parquet").master("local[2]").getOrCreate()
    session.sparkContext.setLogLevel("WARN")//设置日志级别为WARN
    val fsUri = new URI(hdfsRoot)//传递hdfs路径参数
    val fs = FileSystem.get(fsUri, new Configuration())
    val path = hdfsRoot + "/YXFK/compute/KH_JLD"
    val has = fs.exists(getPath(path))
    if(has){
      //读取hdfs文件系统parquet(path)
      val dataFrame = session.read.parquet(path)
         dataFrame.show(10)
         // 筛选，过滤数据
         val result = dataFrame.select("JLDBH", "JLDDZ", "JLDMC", "JLFSDM", "CJSJ")
           .filter("JLDDZ is not null AND JLFSDM = 3")
           .sort("JLDBH")
         result.show(10)
         // 写入部分数据到本地
         result.write.mode(SaveMode.Overwrite).parquet("E:\\result")
       }
     // 读取本地parquet数据
     val localDataFrame = session.read.parquet("E:\\jld.parquet")
     localDataFrame.show(10)
     // 读取写入数据验证
     val resultSpace = session.read.parquet("E:\\result")
     resultSpace.show(10)
   }

  //获取HDFS路径
  def getPath(path:String): Path ={
    if(path.toLowerCase().startsWith("hdfs://")){
      new Path(path)
    }else{
      new Path(hdfsRoot+path)
    }
  }
}
