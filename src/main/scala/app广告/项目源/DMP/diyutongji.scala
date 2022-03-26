package app广告.项目源.DMP

import org.apache.spark.sql.SparkSession

object diyutongji {
  def main(args: Array[String]): Unit = {
  //创建spark session对象
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("save").getOrCreate()
//    val contxt = spark.sparkContext.textFile("")
//      .map(
//        line =>{
//          (_,1)
//        }
//      )


  }
}
