package com.sxkj.flink.logAudit

import com.sxkj.utils.{PropertiesUtils, StreamUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description 日志审计主类
  */
object StartServer {
  /**
    * @Description 主方法，运行日志审计的流
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:51
    * @param
    * @return_type 默认
    * @updateInfo
    */
  def main(args: Array[String]): Unit = {

    //获取当前的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //job失败重启策略，重试4次，每10s一次
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    StreamUtil.baseStream(env)
    StreamUtil.supplyStream(env)
    StreamUtil.systemStream(env)
    StreamUtil.impalaStream(env)
    StreamUtil.hiveStream(env)

    env.execute()

    //每半小时发送一条数据给topic，防止flink和kafka断开
//    val baseTopic = PropertiesUtils.loadLogAuditProperties("baseTopic")
//    val supplyTopic = PropertiesUtils.loadLogAuditProperties("supplyTopic")
//    val systemTopic = PropertiesUtils.loadLogAuditProperties("systemTopic")
//    val impalaTopic = PropertiesUtils.loadLogAuditProperties("impalaTopic")
//    SendMessageToTopic.sendToTopic(baseTopic,supplyTopic,systemTopic,impalaTopic)

  }
}
