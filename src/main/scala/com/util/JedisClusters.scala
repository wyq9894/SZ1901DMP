package com.util
/*
集群jedis工具类
 */
import java.util
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

case class JedisClusters() {

  def JedisC:JedisCluster={
    val config = new JedisPoolConfig()
    config.setMaxTotal(20)
    config.setMaxIdle(10)
    config.setMaxWaitMillis(10)

    // 集群模式
    val nodes = new util.HashSet[HostAndPort]()

    val hostAndPort1 = new HostAndPort("192.168.132.3", 7001)
    val hostAndPort2 = new HostAndPort("192.168.132.3", 7002)
    val hostAndPort3 = new HostAndPort("192.168.132.3", 7003)

    nodes.add(hostAndPort1)
    nodes.add(hostAndPort2)
    nodes.add(hostAndPort3)
    val jedis=new JedisCluster(nodes,config)
    jedis
  }
}
