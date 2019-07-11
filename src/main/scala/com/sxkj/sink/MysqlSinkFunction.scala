package com.sxkj.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

/**
  * @Author:jixiaolong
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description Flink处理数据量统计Sink到MySQL
  */
class MysqlSinkFunction(url: String, user: String, pwd: String) extends RichSinkFunction[(String,Long,String)] {
  var conn: Connection = _
  var p: PreparedStatement = _
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @Description 重写SinkFunction的open方法，打开需要的资源连接
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:13
    * @param Configuration
    * @return_type Unit
    * @updateInfo
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
    conn.setAutoCommit(false)
  }

  /**
    * @Description 重写SinkFunction的invoke方法，数据的sink主方法
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:23
    * @param String ,SinkFunction.Context[_]
    * @return_type Unit
    * @updateInfo
    */
  override def invoke(value: (String,Long,String), context: SinkFunction.Context[_]): Unit = {

    //TODO
    logger.info("[Writed-Redis-Counts]:{}", value._2)
    p = conn.prepareStatement("insert into table() values(?,?,?)")
    p.setString(1, value._1)
    p.setLong(2,value._2)
    p.setString(3,value._3)
    p.execute()
    conn.commit()
  }

  /**
    * @Description 重写SinkFunction的close方法，用来关闭连接资源
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:24
    * @param 无
    * @return_type Unit
    * @updateInfo
    */
  override def close(): Unit = {
    super.close()
    if (p != null) p.close()
    if (conn != null) conn.close()
  }

}
