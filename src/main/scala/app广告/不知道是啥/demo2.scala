package app广告.不知道是啥

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object demo2 {
  def main(args: Array[String]): Unit = {

    val sparksql = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val sc = new SparkContext(sparksql)
    val session = SparkSession.builder().config(sparksql).getOrCreate()
    //一、先将文件转换为json格式
    //1、定义相应的Schema
    val fields = Seq(
      StructField("sessionid", StringType, true),
      StructField("advertisersid", StringType, true),
      StructField("adorderid", StringType, true),
      StructField("adcreativeid", StringType, true),
      StructField("adplatformproviderid", StringType, true),
      StructField("sdkversion", StringType, true),
      StructField("adplatformkey", StringType, true),
      StructField("putinmodeltype", StringType, true),
      StructField("requestmode", StringType, true),
      StructField("adprice", StringType, true),
      StructField("adppprice", StringType, true),
      StructField("requestdate", StringType, true),
      StructField("ip", StringType, true),
      StructField("appid", StringType, true),
      StructField("appname", StringType, true),
      StructField("uuid", StringType, true),
      StructField("device", StringType, true),
      StructField("client", StringType, true),
      StructField("osversion", StringType, true),
      StructField("density", StringType, true),
      StructField("pw", StringType, true),
      StructField("ph", StringType, true),
      StructField("longitude", StringType, true),
      StructField("lat", StringType, true),
      StructField("provincename", StringType, true),
      StructField("cityname", StringType, true),
      StructField("ispid", StringType, true),
      StructField("ispname", StringType, true),
      StructField("networkmannerid", StringType, true),
      StructField("networkmannername", StringType, true),
      StructField("iseffective", StringType, true),
      StructField("isbilling", StringType, true),
      StructField("adspacetype", StringType, true),
      StructField("adspacetypename", StringType, true),
      StructField("devicetype", StringType, true),
      StructField("processnode", StringType, true),
      StructField("apptype", StringType, true),
      StructField("district", StringType, true),
      StructField("paymode", StringType, true),
      StructField("isbid", StringType, true),
      StructField("bidprice", StringType, true),
      StructField("winprice", StringType, true),
      StructField("iswin", StringType, true),
      StructField("cur", StringType, true),
      StructField("rate", StringType, true),
      StructField("cnywinprice", StringType, true),
      StructField("imei", StringType, true),
      StructField("imei", StringType, true),
      StructField("mac", StringType, true),
      StructField("idfa", StringType, true),
      StructField("openudid", StringType, true),
      StructField("androidid", StringType, true),
      StructField("rtbprovince", StringType, true),
      StructField("rtbcity", StringType, true),
      StructField("rtbdistrict", StringType, true),
      StructField("rtbstreet", StringType, true),
      StructField("storeurl", StringType, true),
      StructField("realip", StringType, true),
      StructField("isqualityapp", StringType, true),
      StructField("bidfloor", StringType, true),
      StructField("aw", StringType, true),
      StructField("ah", StringType, true),
      StructField("imeimd5", StringType, true),
      StructField("macmd5", StringType, true),
      StructField("idfamd5", StringType, true),
      StructField("openudidmd5", StringType, true),
      StructField("androididmd5", StringType, true),
      StructField("imeisha1", StringType, true),
      StructField("macsha1", StringType, true),
      StructField("idfasha1", StringType, true),
      StructField("openudidsha1", StringType, true),
      StructField("androididsha1", StringType, true),
      StructField("uuidunknow", StringType, true),
      StructField("userid", StringType, true),
      StructField("iptype", StringType, true),
      StructField("initbidprice", StringType, true),
      StructField("adpayment", StringType, true),
      StructField("agentrate", StringType, true),
      StructField("lomarkrate", StringType, true),
      StructField("adxrate", StringType, true),
      StructField("title", StringType, true),
      StructField("keywords", StringType, true),
      StructField("tagid", StringType, true),
      StructField("callbackdate", StringType, true),
      StructField("channelid", StringType, true),
      StructField("mediatype", StringType))
    //    fields.foreach(println)
    //2、定制Schema
    val mySchema = StructType(fields)
    //3、读入数据产生df
    val carsrdd = session.sparkContext.textFile("C:\\Users\\Public\\Nwt\\cache\\recv\\zbc\\互联网广告\\互联网广告第一天\\2016-10-01_06_p1_invalid.1475274123982.log")
    //4、分割数据
    //    val rowrdd = carsrdd.map(
    //      line =>{
    //        line.split(",")
    //        Row( line(0).toString,
    //          line(1).toString,
    //          line(2).toString,
    //          line(3).toString,
    //          line(4).toString,
    //          line(5).toString,
    //          line(6).toString,
    //          line(7).toString,
    //          line(8).toString,
    //          line(9).toString,
    //          line(10).toString,
    //          line(11).toString,
    //          line(12).toString,
    //          line(13).toString,
    //          line(14).toString,
    //          line(15).toString,
    //          line(16).toString,
    //          line(17).toString,
    //          line(18).toString,
    //          line(19).toString,
    //          line(20).toString,
    //          line(21).toString,
    //          line(22).toString,
    //          line(23).toString,
    //          line(24).toString,
    //          line(25).toString,
    //          line(26).toString,
    //          line(27).toString,
    //          line(28).toString,
    //          line(29).toString,
    //          line(30).toString,
    //          line(31).toString,
    //          line(32).toString ,
    //          line(33).toString,
    //          line(34).toString,
    //          line(35).toString ,
    //          line(36).toString ,
    //          line(37).toString,
    //          line(38).toString,
    //          line(39).toString,
    //          line(40).toString,
    //          line(41).toString ,
    //          line(42).toString ,
    //          line(43).toString,
    //          line(44).toString,
    //          line(45).toString ,
    //          line(46).toString,
    //          line(47).toString,
    //          line(48).toString,
    //          line(49).toString,
    //          line(50).toString,
    //          line(51).toString,
    //          line(52).toString,
    //          line(53).toString,
    //          line(54).toString,
    //          line(55).toString,
    //          line(56).toString,
    //          line(57).toString,
    //          line(58).toString,
    //          line(59).toString,
    //          line(60).toString ,
    //          line(61).toString,
    //          line(62).toString,
    //          line(63).toString,
    //          line(64).toString,
    //          line(65).toString,
    //          line(66).toString,
    //          line(67).toString,
    //          line(68).toString,
    //          line(69).toString,
    //          line(70).toString,
    //          line(71).toString,
    //          line(72).toString,
    //          line(73).toString,
    //          line(74).toString,
    //          line(75).toString,
    //          line(76).toString,
    //          line(77).toString,
    //          line(78).toString,
    //          line(79).toString,
    //          line(80).toString,
    //          line(81).toString,
    //          line(82).toString)
    //      }
    //    )
    //4、分割数据
    val rowrdd = carsrdd.map(_.split(",")).map(line => Row(
      line(0).trim.toString,
      line(1).trim.toString,
      line(2).trim.toString,
      line(3).trim.toString,
      line(4).trim.toString,
      line(5).trim.toString,
      line(6).trim.toString,
      line(7).trim.toString,
      line(8).trim.toString,
      line(9).trim.toString,
      line(10).trim.toString,
      line(11).trim.toString,
      line(12).trim.toString,
      line(13).trim.toString,
      line(14).trim.toString,
      line(15).trim.toString,
      line(16).trim.toString,
      line(17).trim.toString,
      line(18).trim.toString,
      line(19).trim.toString,
      line(20).trim.toString,
      line(21).trim.toString,
      line(22).trim.toString,
      line(23).trim.toString,
      line(24).trim.toString,
      line(25).trim.toString,
      line(26).trim.toString,
      line(27).trim.toString,
      line(28).trim.toString,
      line(29).trim.toString,
      line(30).trim.toString,
      line(31).trim.toString,
      line(32).trim.toString,
      line(33).trim.toString,
      line(34).trim.toString,
      line(35).trim.toString,
      line(36).trim.toString,
      line(37).trim.toString,
      line(38).trim.toString,
      line(39).trim.toString,
      line(40).trim.toString,
      line(41).trim.toString,
      line(42).trim.toString,
      line(43).trim.toString,
      line(44).trim.toString,
      line(45).trim.toString,
      line(46).trim.toString,
      line(47).trim.toString,
      line(48).trim.toString,
      line(49).trim.toString,
      line(50).trim.toString,
      line(51).trim.toString,
      line(52).trim.toString,
      line(53).trim.toString,
      line(54).trim.toString,
      line(55).trim.toString,
      line(56).trim.toString,
      line(57).trim.toString,
      line(58).trim.toString,
      line(59).trim.toString,
      line(60).trim.toString,
      line(61).trim.toString,
      line(62).trim.toString,
      line(63).trim.toString,
      line(64).trim.toString,
      line(65).trim.toString,
      line(66).trim.toString,
      line(67).trim.toString,
      line(68).trim.toString,
      line(69).trim.toString,
      line(70).trim.toString,
      line(71).trim.toString,
      line(72).trim.toString,
      line(73).trim.toString,
      line(74).trim.toString,
      line(75).trim.toString,
      line(76).trim.toString,
      line(77).trim.toString,
      line(78).trim.toString,
      line(79).trim.toString,
      line(80).trim.toString,
      line(81).trim.toString,
      line(82).trim.toString
      //        line(83).trim.toString
      //        line(83).trim.toString,
      //        line(84).trim.toString,
      //        line(85).trim.toString
    ))
    //表头和数据拼接
    val carsDf = session.createDataFrame(rowrdd, mySchema)
    //查看数据
    carsDf.show()
    //    session.sparkContext.parallelize(rowrdd)
    //    val frame1 = session.sparkContext.parallelize(fields).toDF()
    //5、表头和数据拼接
    //    val carsDF = session.createDataFrame(rowrdd,mySchema)
    //        carsDF.show()
    //      carsDF.show()
    //    println(fields.length)
    //    rowrdd.collect().foreach(println)


  }
}
