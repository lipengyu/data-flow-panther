package com.sxkj.utils

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties
/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description 加载配置文件
  */
object PropertiesUtils {
  val directory = "/opt/Apps/panther/conf/logAudit.properties"
  val buriedPath = "/opt/Apps/panther/conf/BuriedPoint.properties"

  /**
    * @Description 加载日志审计的配置文件内容
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:27
    * @param 无
    * @return_type String
    * @updateInfo
    */
  def loadLogAuditProperties(key: String): String = {
    val properties = new Properties()
    val in = new BufferedInputStream(new FileInputStream(directory))
    properties.load(in)
    in.close()
    properties.getProperty(key)
  }


  /**
    * @Description 加载埋点的配置文件内容
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:41
    * @param null
    * @return_type String
    * @updateInfo
    */
  def loadBuriedPointProperties(key: String): String = {
    val properties = new Properties()
    val inBur = new BufferedInputStream(new FileInputStream(buriedPath))
    properties.load(inBur)
    inBur.close()
    properties.getProperty(key)
  }

}
