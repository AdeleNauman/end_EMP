package app广告.项目源.DMP

import javafx.scene.input.DataFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object 测试数据转换为parquet {
  def main(args: Array[String]): Unit = {
    //1、引入环境
    //使用spark转换json日志文件为parquet方式
    val spark = SparkSession.builder().appName("session").master("local[*]").getOrCreate()
    //   阅读格式设置为json数据
    val df = spark.read.format("json").load("date/a.json")//转换的parquet版本会与kafka依赖冲突 使用2.6的版本即可
    df.write.format("parquet").save("output1")
    df.printSchema()
    df.show()
    //2、关闭资源
    spark.stop()
  }

}
