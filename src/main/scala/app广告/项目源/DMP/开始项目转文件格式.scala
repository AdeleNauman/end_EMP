package app广告.项目源.DMP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import javax.swing.text.DateFormatter

object 开始项目转文件格式 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("session")
      .master("local[*]")
      .appName("aa")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val rdd = sc.textFile("date/newP")

    sc.stop()
  }

  case class Ads(
                  client: Int,
                  osversion: String,
                  density: String,
                  pw: Int,
                  ph: Int,
                  long: String,
                  lat: String,
                  provincename: String,
                  cityname: String,
                  ispid: Int,
                  ispname: String,
                  networkmannerid: Int,
                  networkmannername:String,
                  iseffective: Int,
                  isbilling: Int,
                  adspacetype: Int,
                  adspacetypename: String,
                  devicetype: Int,
                  processnode: Int,
                  apptype: Int,
                  district: String,
                  paymode: Int,
                  isbid: Int,
                  bidprice: Double,
                  winprice: Double,
                  iswin: Int,
                  cur: String,
                  rate: Double,
                  cnywinprice: Double,
                  imei: String,
                  mac: String,
                  idfa: String,
                  openudid: String,
                  androidid: String,
                  rtbprovince: String,
                  rtbcity: String,
                  rtbdistrict: String,
                  rtbstreet: String,
                  storeurl: String,
                  realip: String,
                  isqualityapp: Int,
                  bidfloor: Double,
                  aw: Int,
                  ah: Int,
                  imeimd5: String,
                  macmd5: String,
                  idfamd5: String,
                  openudidmd5: String,
                  androididmd5: String,
                  imeisha1: String,
                  macsha1: String,
                  idfasha1: String,
                  openudidsha1: String,
                  androididsha1: String,
                  uuidunknow: String,
                  userid: String,
                  iptype: Int,
                  initbidprice: Double,
                  adpayment: Double,
                  agentrate: Double,
                  lomarkrate: Double,
                  adxrate: Double,
                  title: String,
                  keywords: String,
                  tagid: String,
                  callbackdate: String,
                  channelid: String,
                  mediatype: Int
                )
}
