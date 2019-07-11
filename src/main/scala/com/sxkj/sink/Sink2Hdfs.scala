package com.sxkj.sink

import com.sxkj.utils.{DateTimeBucketer, PropertiesUtils}
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description flink写入hdfs
  */
object Sink2Hdfs {
  /**
    * @Description flink sink to hdfs
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:52
    * @param 需要写入的hdfs地址
    * @return_type BucketingSink[String]
    * @updateInfo
    */
  def toHdfsPath(hdfsPath: String): BucketingSink[String] = {

    val sink2hdfs = new BucketingSink[String](hdfsPath)

    sink2hdfs.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd"))
    // this is 30 mins
    sink2hdfs.setBatchRolloverInterval(PropertiesUtils.loadLogAuditProperties("batchRolloverInt").toInt)
    // this is 100 MB
    sink2hdfs.setBatchSize(PropertiesUtils.loadLogAuditProperties("batchSize").toInt)

    //设置的是检查两次检查桶不活跃的情况的周期
    sink2hdfs.setInactiveBucketThreshold(10 * 60 * 1000L)
    //设置的是关闭不活跃桶的阈值,多久时间没有数据写入就关闭桶
    sink2hdfs.setInactiveBucketCheckInterval(10 * 60 * 1000L)

    sink2hdfs
  }
}
