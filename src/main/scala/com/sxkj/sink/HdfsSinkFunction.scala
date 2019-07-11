package com.sxkj.sink

import java.util.Date

import com.sxkj.utils.{DateTimeBucketer, PropertiesUtils}
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
  * @Author:jixiaolong
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description Flink流数据Sink到HDFS
  */
class HdfsSinkFunction {

  //Redis属性
  val redis_host = PropertiesUtils.loadBuriedPointProperties("REDIS_HOST")
  val redis_port = PropertiesUtils.loadBuriedPointProperties("REDIS_PORT").toInt
  val redis_timeout = PropertiesUtils.loadBuriedPointProperties("redis_timeout").toInt
  val redis_password = PropertiesUtils.loadBuriedPointProperties("redis_password")

  /**
    * @Description 获得hdfs的sink对象
    * @Author:jixiaolong
    * @Date: 2019/5/21 17:37
    * @param String
    * @return_type BucketingSink[String]
    * @updateInfo
    */
  def jsonFileSinkHDFS(hdfsPath: String) = {
    val sink2hdfs = new BucketingSink[String](hdfsPath)

    sink2hdfs.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd"))
    // this is 30 mins
    sink2hdfs.setBatchRolloverInterval(PropertiesUtils.loadBuriedPointProperties("batchRolloverInt").toInt)
    // this is 100 MB
    sink2hdfs.setBatchSize(PropertiesUtils.loadBuriedPointProperties("batchSize").toInt)

    //设置的是检查两次检查桶不活跃的情况的周期
    sink2hdfs.setInactiveBucketThreshold(10 * 60 * 1000L)
    //设置的是关闭不活跃桶的阈值,多久时间没有数据写入就关闭桶
    sink2hdfs.setInactiveBucketCheckInterval(10 * 60 * 1000L)

    sink2hdfs
  }

}
