package new_appEmp.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisUtil {
  //链接到redis的工具方法
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig,"localhost",6379,30000,null,4)

  def getJedis = jedisPool.getResource
}
