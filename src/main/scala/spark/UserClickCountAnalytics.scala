package spark

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 郭飞 on 2016/5/21.
  */
object UserClickCountAnalytics {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("user_events")
    //本地虚拟机ZK地址
    //val brokers = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    //测试环境ZK地址
    val brokers = "192.168.20.131:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    })

    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          //Hbase配置
          val tableName = "table1"
          val myConf = HBaseConfiguration.create()
          myConf.set("hbase.zookeeper.quorum", "192.168.20.131:9092")
          myConf.set("hbase.zookeeper.property.clientPort", "2181")
          myConf.set("hbase.defaults.for.version.skip", "true")
          val uid = pair._1
          val click = pair._2
          println(uid)
          val p = new Put(Bytes.toBytes(uid))
          p.add("col1".getBytes, "a".getBytes, Bytes.toBytes(click))
          val myTable = new HTable(myConf, TableName.valueOf(tableName))
          myTable.setAutoFlush(false, false)
          myTable.setWriteBufferSize(3*1024*1024)
          myTable.put(p)
          myTable.flushCommits()
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
