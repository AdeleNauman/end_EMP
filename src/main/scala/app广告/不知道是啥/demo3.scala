package app广告.不知道是啥

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object demo3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("name").setMaster("local[*]")
    val sparksql = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val sc = new SparkContext(sparksql)
    val session = SparkSession.builder().config(sparksql).getOrCreate()
    //一、先将文件转换为json格式
    //1、定义相应的Schema
    val fields = Seq(
      StructField("sessionid", IntegerType, true),
      StructField("advertisersid", IntegerType, true))
    //    fields.foreach(println)
    //2、定制Schema
    val mySchema = StructType(fields)
    //3、读入数据产生df
    //    C:\Users\azm\Desktop\1233.txt
    val carsrdd = session.sparkContext.textFile("C:\\\\\\\\Users\\\\\\\\Public\\\\\\\\Nwt\\\\\\\\cache\\\\\\\\recv\\\\\\\\zbc\\\\\\\\互联网广告\\\\\\\\互联网广告第一天\\\\\\\\2016-10-01_06_p1_invalid.1475274123982.log\\")
    //4、切分数据
    val rowrdd = carsrdd.map(_.split(",")).filter(_.length >= 85).map(line => Row(
      line(0).trim.toInt,
      line(1).trim.toInt
    ))
    val carsDF = session.createDataFrame(rowrdd, mySchema)
    carsDF.show()

  }


}
