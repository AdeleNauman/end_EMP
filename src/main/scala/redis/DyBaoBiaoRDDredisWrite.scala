package Territory
import com.dahua.utils.{LogBean, redisUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
object DyBaoBiaoRDDredisWrite {
  def main(args: Array[String]): Unit = {
    /**
     * 写入redis 保存
     */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DmpLogEtlParquet")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.textFile("data/app_mappings.txt").map(line=>{
      val str: Array[String] = line.split("[:]", -1) // -1代表一直读到借书
      (str(0),str(1))
    }).foreachPartition(ite=>{ //分区写入

      val jedis: Jedis = redisUtil.getJedis

      ite.foreach(mapping=>{
        jedis.set(mapping._1,mapping._2)
      })

      jedis.close()

    })


  }
}
