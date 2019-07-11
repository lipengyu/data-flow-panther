package com.sxkj.sink

import java.text.DecimalFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sxkj.flink.buriedPoint.FlinkDealWithMallLog.dateFormat
import com.sxkj.utils.PropertiesUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
  * @Author:jixiaolong
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description Flink流数据Sink到Hbase
  */
class HbaseSinkFunction(business: String, row_key: String, tableName: String, columnFamily: String, backupFamily: String, zookeeperConn: String, regionNum: Int) extends RichSinkFunction[String] {


  val logger = LoggerFactory.getLogger(this.getClass)

  //Redis属性
  val redis_host = PropertiesUtils.loadBuriedPointProperties("REDIS_HOST")
  val redis_port = PropertiesUtils.loadBuriedPointProperties("REDIS_PORT").toInt
  val redis_timeout = PropertiesUtils.loadBuriedPointProperties("redis_timeout").toInt
  val redis_password = PropertiesUtils.loadBuriedPointProperties("redis_password")

  //定义全局可变变量
  var conn: Connection = _
  // 批次计数器
  var count: Long = 0
  // 累计计数器
  var total: Long = 0
  // 批量写入的大小，默认100
  var batchSize = 100
  //Jedis配置对象
  var pool: JedisPool = _
  //Jedis对象
  var jedis: Jedis = _
  // 用于保存批量存入hbase的list
  var puts = new util.ArrayList[Put]

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
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperConn)
    conf.setInt("hbase.hconnection.threads.max", 256)
    conf.setInt("hbase.hconnection.threads.core", 32)
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(conf)

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
    pool = new JedisPool(poolconf, redis_host, redis_port, redis_timeout, redis_password)
    jedis = pool.getResource
  }

  /**
    * @Description 重写SinkFunction的invoke方法，数据的sink处理逻辑
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:27
    * @param String ,SinkFunction.Context[_]
    * @return_type Unit
    * @updateInfo
    */
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val mallJson = JSON.parseObject(value)

    val table = conn.getTable(TableName.valueOf(tableName))

    //RowKey设计,反转+time:避免热点问题,好取数
    //val rowkey: String = (scala.util.Random.nextInt(regionNum - 1).toString) + mallJson.get("distinct_id").toString + mallJson.get("time").toString

    //反转
    //val rowkey: String = mallJson.get("distinct_id").toString.reverse + mallJson.get("time").toString

    //uuid + 两位随机数前缀 做为rowkey
    //    val df = new DecimalFormat("00")
    //    val rowkey: String = df.format(scala.util.Random.nextInt(100)) + mallJson.get("properties_uuid").toString


    var rowkey = ""
    if (business.equals("etd_behav_user")) {
      //uuid反转作为rowkey
      rowkey = mallJson.get(row_key).toString.reverse
    } else if (business.equals("etd_behav_bma")) {
      //uuid + 两位随机数前缀 做为rowkey
      val df = new DecimalFormat("00")
      rowkey = df.format(scala.util.Random.nextInt(100)) + mallJson.get("properties_uuid").toString
    } else {
      System.out.println("incognizant business!")
    }


    val put = new Put(Bytes.toBytes(rowkey))

    val jsonKeys = mallJson.keySet()
    val iter = jsonKeys.iterator
    while (iter.hasNext) {
      val key = iter.next()
      val value = mallJson.get(key)
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(value.toString))
    }

    /*
    //单独备份数据，供数仓取数
    put.addColumn(Bytes.toBytes(backupFamily), Bytes.toBytes(columnFamily), Bytes.toBytes(value))
*/
    //批量插入模块
    count += 1
    total += 1

    /*
    //单独添加
    table.put(put)
    logger.info("[Writed-Hbase-Counts]:{}", total)
    table.close()
    */

    //批量添加
    if (count < batchSize) puts.add(put) else {
      table.put(puts)
      jedis.set("FHB" + "||" + business + "||" + dateFormat.format(new Date().getTime), total.toString)
      logger.info("[Writed-Hbase-Counts]:{}", total)
      count = 0
      puts.clear()
      table.close()
    }

    if (total == Long.MaxValue) {
      logger.info("[Writed-Total-Counts]:{}", total)
      total = 0
    }

  }

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
    if (conn != null) conn.close()
    if (pool != null) pool.close()
    if (jedis != null) jedis.close()
  }

}
