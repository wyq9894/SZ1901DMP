package com.util
/*
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConn {

  private val config: JedisPoolConfig =new JedisPoolConfig
  config.setMaxIdle(20)
  config.setMaxTotal(10)


  private val pool = new JedisPool(config, "192.168.132.61", 6379)

  def getConn(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedis = getConn
    jedis.auth("123")
    jedis.close()
  }
}
*/