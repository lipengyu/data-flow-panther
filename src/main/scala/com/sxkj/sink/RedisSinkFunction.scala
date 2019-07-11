package com.sxkj.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @Author:jixiaolong
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description Flink处理数据量统计Sink到Redis
  */
class RedisSinkFunction(host: String, port: Int, timeout: Int, password: String) extends RichSinkFunction[(String, String)] {

  val logger = LoggerFactory.getLogger(this.getClass)
  //定义全局可变变量
  var jedis: Jedis = _
  var pool: JedisPool = _

  /**
    * @Description 重写SinkFunction的open方法，打开需要的资源连接
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:26
    * @param Configuration
    * @return_type Unit
    * @updateInfo
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val poolconf = new JedisPoolConfig
    poolconf.setMaxTotal(5000)
    poolconf.setMaxWaitMillis(5000L)
    poolconf.setMaxIdle(256)
    poolconf.setTestOnBorrow(true)
    poolconf.setTestOnReturn(true)
    poolconf.setTestWhileIdle(true)
    poolconf.setMinEvictableIdleTimeMillis(60000L)
    poolconf.setTimeBetweenEvictionRunsMillis(3000L)
    poolconf.setNumTestsPerEvictionRun(-1)
    pool = new JedisPool(poolconf, host, port, timeout, password)
    jedis = pool.getResource
  }

  /**
    * @Description 重写SinkFunction的invoke方法，数据的sink主方法
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:28
    * @param (String , String),SinkFunction.Context[_]
    * @return_type
    * @updateInfo
    */
  override def invoke(value: (String, String), context: SinkFunction.Context[_]): Unit = {
    //TODO
    logger.info("[Writed-Redis-Counts]:{}", value._2)
    jedis.set(value._1, value._2)
  }

  /*
  //任意类型转String写入Redis
  def set(key: String, value: Any): Unit = {
    jedis.set(key, String.valueOf(value))
  }
*/
  /**
    * @Description 重写SinkFunction的close方法，用来关闭连接资源
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:28
    * @param 无
    * @return_type Unit
    * @updateInfo
    */
  override def close(): Unit = {
    super.close()
    if (jedis != null) jedis.close()
    if (pool != null) pool.close()
  }

}
