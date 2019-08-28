package com.utils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  *Redis 连接池工具包
  */
object  JedisPool {
  //连接配置
  val config= new JedisPoolConfig
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //设置连接池属性分别有： 配置  主机名   端口号  连接超时时间  192.168.239.135
  // localhost 127.0.0.1
  val pool=new JedisPool(config,"cdh",6379,10000)
  //连接池
  def getConnections(): Jedis ={
    pool.getResource
  }
  //关闭连接池
  def close(jedis: Jedis)={
    if (jedis != null) {
      jedis.close
      if (jedis.isConnected) try
        jedis.disconnect
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }
}
