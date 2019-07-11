package com.sxkj.flink.buriedPoint

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sxkj.sink.{EsSinkFunction, HbaseSinkFunction, HdfsSinkFunction, RedisSinkFunction}
import com.sxkj.utils.PropertiesUtils
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.hadoop.fs
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

/**
  * @Author:jixiaolong
  * @Date:2019 /5/31 9:54
  * @Version 1.0
  * @Description
  */
object FlinkDealWithUserMallLog {
  // 定义全局变量
  val logger = LoggerFactory.getLogger(this.getClass)
  //定义一个时间格式
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  var logDataCount: Long = 0
  val business = "etd_behav_user"

  /**
    * @Description 埋点数据处理主函数
    * @Author: songlei
    * @Date: 2019/5/15 16:50
    * @param Array [String]
    * @return_type Unit
    * @updateInfo jixiaolong 2019/5/20 9:20
    *             增加了计数流
    */
  def main(args: Array[String]): Unit = {

    //获取当前的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //job失败重启策略，重试4次，每10s一次
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    //kafka consumer
    val kafkaProps = new Properties()
    val kafkaTopic = PropertiesUtils.loadBuriedPointProperties("buriedPointUserTopic")

    //连接zookeeper
    kafkaProps.setProperty("zookeeper.connect", PropertiesUtils.loadBuriedPointProperties("zookeeperHost"))
    //连接kafkaBroker
    kafkaProps.setProperty("bootstrap.servers", PropertiesUtils.loadBuriedPointProperties("kafkaBroker"))
    //配置kerberos的三个重要属性
    kafkaProps.setProperty("group.id", PropertiesUtils.loadBuriedPointProperties("buriedPointUserConsumerGroup"))
    //超时时间
    kafkaProps.setProperty("transaction.timeout.ms", 3 * 24 * 60 * 60 * 1000 + "")

    //设置Kerberos属性
    val kafkaKerberos = PropertiesUtils.loadBuriedPointProperties("kafkaKerberos")
    if (kafkaKerberos != null && !"".equals(kafkaKerberos)) {
      System.setProperty("java.security.krb5.conf", PropertiesUtils.loadBuriedPointProperties("krbs"))
      System.setProperty("java.security.auth.login.config", PropertiesUtils.loadBuriedPointProperties("jaas"))
      kafkaProps.setProperty("security.protocol", PropertiesUtils.loadBuriedPointProperties("securityProtocol"))
      kafkaProps.setProperty("sasl.kerberos.service.name", kafkaKerberos)
    }


    //消费对应topic中的数据
    val kafkaConsumer = new FlinkKafkaConsumer010[String](kafkaTopic, new SimpleStringSchema(), kafkaProps)
    //设置并行度
    val sinkParallelism = PropertiesUtils.loadBuriedPointProperties("sinkParallelism").toInt
    val sourceParallelism = PropertiesUtils.loadBuriedPointProperties("sourceParallelism").toInt

    //HBase属性
    val tableName = PropertiesUtils.loadBuriedPointProperties("hbase_tableNameUser")
    val columnFamily = PropertiesUtils.loadBuriedPointProperties("hbase_columnFamily")
    val backupFamily = PropertiesUtils.loadBuriedPointProperties("hbase_backupFamily")
    val zookeeperConn = PropertiesUtils.loadBuriedPointProperties("zookeeperHost")
    val regionNum = PropertiesUtils.loadBuriedPointProperties("hbase_regionNum").toInt
    //Redis属性
    val redis_host = PropertiesUtils.loadBuriedPointProperties("REDIS_HOST")
    val redis_port = PropertiesUtils.loadBuriedPointProperties("REDIS_PORT").toInt
    val redis_timeout = PropertiesUtils.loadBuriedPointProperties("redis_timeout").toInt
    val redis_password = PropertiesUtils.loadBuriedPointProperties("redis_password")

    //Flink对接Kafka
    val transac = env.addSource(kafkaConsumer).setParallelism(sourceParallelism).name("Flink Source Kafka")

    /*
    // 计数流
    val cnts = stream_count(transac)
    //将计数数据Sink到Redis
    val redisSink = new RedisSinkFunction(redis_host, redis_port, redis_timeout, redis_password)
    cnts.addSink(redisSink).name("Flink Sink Redis Counts").setParallelism(sinkParallelism)
*/
    //过滤数据,去除$符号,判断是否有记录唯一标识uuid,是否可json化
    val FilterData = transac.map(line => line.replaceAll("\\$", "")).
      //&& line.contains("time")
      filter(line => line.contains("id") && isJSONObject(line))

    //将拿到的kafka数据转换成json
    val DF2Json2 = FilterData.map(item => JSON.parseObject(item))

    //将拿到的json拉平,规整,返回展平的json
    val correctDataJson = DF2Json2.map(line => {
      val newJson = dataRegulation(expandJson(null, line, new JSONObject))

      //TODO解压&解密

      //return
      newJson.toJSONString
    })


    /*
    //校验流
    val check_stream = check_json(correctDataJson)
    check_stream.addSink(redisSink).name("Flink Sink Redis JsonData").setParallelism(1)
*/
    /*
        val mysqlCnts = stream_count2mysql(transac)
        val mysqlSink = new MysqlSinkFunction("jdbc:mysql://172.17.0.198:3306/test_oss?user=dispatch&password=dispatch2019&useUnicode=true&characterEncoding=gbk&autoReconnect=true&failOverReadOnly=false", "dispatch", "dispatch2019")
        mysqlCnts.addSink(mysqlSink).name("Flink Sink MySQL").setParallelism(1)
    */


    // 将数据Sink到HDFS
    val hdfsPath = PropertiesUtils.loadBuriedPointProperties("hdfsPathBuriedPointUser")
    val HdfsSink = new HdfsSinkFunction()
    correctDataJson.addSink(HdfsSink.jsonFileSinkHDFS(hdfsPath)).name("Flink Sink HDFS").setParallelism(sinkParallelism)

    // 将数据Sink到HBase
    val HBaseSink = new HbaseSinkFunction(business, "id", tableName, columnFamily, backupFamily, zookeeperConn, regionNum)
    correctDataJson.addSink(HBaseSink).name("Flink Sink HBase").setParallelism(sinkParallelism)

    //将数据Sink到ES，从配置文件获取ES的索引名
    val es_indexName = PropertiesUtils.loadBuriedPointProperties("es_indexNameUser")
    val EsSink = new EsSinkFunction(es_indexName)
    correctDataJson.addSink(EsSink.esSinkBuilder.build()).name("Flink Sink ES").setParallelism(sinkParallelism)

    env.execute()

  }

  /**
    * @Description 判断字符串是否可解析为Json
    * @Author: songlei
    * @Date: 2019/5/15 16:50
    * @param String
    * @return_type Boolean
    * @updateInfo
    */
  def isJSONObject(str: String) = try {
    JSON.parseObject(str)
    true
  } catch {
    case e: Exception => {
      // 保留格式错误的数据
      logger.error(e.getMessage + "\t" + str)
      false
    }
  }

  /**
    * @Description 将json数据value规整
    * @Author: songlei
    * @Date: 2019/5/15 16:50
    * @param JSONObject
    * @return_type JSONObject
    * @updateInfo
    */
  def dataRegulation(temJson: JSONObject) = {
    //去除uuid中"-"
    val propertiesUuid = temJson.get("properties_uuid")
    if (propertiesUuid != null) temJson.put("properties_uuid", propertiesUuid.toString.replaceAll("\\-", ""))
    //去除user_id中"-",个别字段中不包含该字段导致出错
    val distinctId = temJson.get("distinct_id")
    if (distinctId != null) temJson.put("distinct_id", distinctId.toString.replaceAll("\\-", ""))
    temJson
  }


  /**
    * @Description 通过window将数据流计数统计
    * @Author:jixiaolong
    * @Date: 2019/5/21 16:10
    * @param DataStream [String]
    * @return_type DataStream[(String,String)]
    * @updateInfo
    */
  def stream_count(correctDataJson: DataStream[String]) = {
    //计数流
    correctDataJson.map(_ => {
      logDataCount += 1
      logDataCount
    }).timeWindowAll(Time.seconds(10)).reduce((x, y) => x)
      .map(s => {
        logger.info("[Flink-Window-Counts]:{}", s)
        ("KFL" + "||" + business + "||" + dateFormat.format(new Date().getTime), s.toString)
      })
  }

  /**
    * @Description 通过window将数据流计数统计
    * @Author:jixiaolong
    * @Date: 2019/5/21 16:10
    * @param DataStream [String]
    * @return_type DataStream[(String,String)]
    * @updateInfo
    */
  def stream_count2mysql(correctDataJson: DataStream[String]) = {
    //计数流
    correctDataJson.map(_ => {
      logDataCount += 1
      logDataCount
    }).timeWindowAll(Time.seconds(10)).reduce((x, y) => x)
      .map(s => {
        logger.info("[Flink-Window-Counts]:{}", s)
        ("KFL" + "||" + business, s, dateFormat.format(new Date().getTime))
      })
  }

  /**
    * @Description 按固定时间跨度抓取json送到kafka进行数据校验
    * @Author:jixiaolong
    * @Date: 2019/5/22 15:33
    * @param DataStream [String]
    * @return_type DataStream[String]
    * @updateInfo
    */
  def check_json(correctDataJson: DataStream[String]) = {
    //校验抽取json流
    correctDataJson.countWindowAll(1000).reduce((x, y) => x)
      .map(m => {
        ("user_id" + dateFormat.format(new Date().getTime), m)
      })
  }

  /**
    * @Description 将json写parquet文件
    * @Author: songlei
    * @Date: 2019/5/15 16:50
    * @param 无
    * @return_type ParquetWriter[GenericRecord]
    * @updateInfo
    */
  def jsonFileSinkHDFSByParquet() = {
    val dateTimeBucketer = new Date()
    val fdf = FastDateFormat.getInstance("yyyy-MM-dd")
    val dateTimeBucketerStr = fdf.format(dateTimeBucketer)
    val schemaString = "{\"namespace\": \"com.weadmin.flink\",\"type\": \"record\",\"name\": \"maipoint\",\"fields\": " +
      "[{\"name\":\"lib\",\"type\":\"string\"},{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"string\"}]}"
    val schema = new org.apache.avro.Schema.Parser().parse(schemaString)
    val hdfsPath = s"hdfs:///home/buried_data/dt=${dateTimeBucketerStr}"
    val path = new fs.Path(hdfsPath)
    val parquetWriter = AvroParquetWriter.builder[GenericRecord](path)
      .withSchema(schema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      //.withPageSize()
      //.withRowGroupSize()
      .withDictionaryEncoding(true)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withValidation(true)
      .build()
    //parquetWriter.write()
    parquetWriter.close()
    parquetWriter
  }


  /**
    * @Description 压平多级json，将key全部转为小写
    * @Author: songlei
    * @Date: 2019/5/15 16:50
    * @param String ,JSONObject,JSONObject
    * @return_type JSONObject
    * @updateInfo jixiaolong 2019/5/21 16:53
    *             更改了递归传入的第一个参数，避免了三层及以上json的key无法完全拼接的问题
    */
  def expandJson(previousKey: String, multJson: JSONObject, temJson: JSONObject): JSONObject = {
    val jsonKeys = multJson.keySet()
    val iter = jsonKeys.iterator
    while (iter.hasNext) {
      val key = iter.next()
      val value = multJson.get(key)
      //获取字段类型
      //val valueType = value.getClass.getSimpleName
      if (value.isInstanceOf[JSONObject]) {
        val valueJson = JSON.parseObject(value.toString)
        expandJson(if (previousKey == null) "" + key else previousKey + "_" + key, valueJson, temJson)
      }
      else if (null == previousKey)
        temJson.put(key.toLowerCase(), value)
      else {
        val mulkey = previousKey + "_" + key
        temJson.put(mulkey.toLowerCase(), value)
      }
    }
    temJson
  }

}
