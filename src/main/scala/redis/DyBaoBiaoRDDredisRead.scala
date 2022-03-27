package Territory

import com.dahua.utils.{LogBean, redisUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DyBaoBiaoRDDredisRead {
  def main(args: Array[String]): Unit = {
    /**
     * 从Redis 读取进行空值的替换
     */
    if (args.length != 1) {
      println("缺少参数")
      sys.exit(0)
    }
    var Array(inputPath) = args
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("DmpLogEtlParquet")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val log: RDD[String] = sc.textFile(inputPath)
    val logRDD: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      !t.appid.isEmpty
    })
    val rdd1: RDD[LogBean] = logRDD.mapPartitions(log => {

      val jedis: Jedis = redisUtil.getJedis

      var res = List[LogBean]()
      while (log.hasNext){
        val bean: LogBean = log.next()
        if (bean.appname == "" || bean.appname.isEmpty) {
            val   str = jedis.get(bean.appid)
          bean.appname = str
        }
        res.::=(bean)
      }
      jedis.close()
      res.iterator
    })
    rdd1.saveAsTextFile("D:\\内网通文件\\白华强\\互联网广告\\输出数据\\redisOUtput")

  }
}
