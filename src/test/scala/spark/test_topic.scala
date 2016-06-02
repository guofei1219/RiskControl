package spark

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 郭飞 on 2016/5/23.
  */
object test_topic {
  def main(args: Array[String]): Unit = {
    var masterUrl = "spark://hc4:7070"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("test_topic_CH0001_R002")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("CH0001_R002")

    //测试环境ZK地址
    val brokers = "192.168.20.131:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {

      val data = JSONObject.fromObject(line._2)
      Some(data)
    })

    events.foreachRDD(rdd=>{
      println("收到信息"+rdd.collect())
    })
  }
}
