package kafka2streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  * Created by hui on 2016/6/3.
  */
object SourceOfKafka {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
                             |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
                             |  <brokers> is a list of one or more Kafka brokers
                             |  <topics> is a list of one or more kafka topics to consume from
                             |  <groupid> is a consume group
         """.stripMargin)
       System.exit(1)
     }
    //val Array(brokers,topics,groupid) = args
    val brokers = "hc4:9092"
    val topics = "user_events"
    val groupid = "me"

    val sparkConf = new SparkConf().setAppName("DirectKafka").setMaster("local[2]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers.toString,
      "group.id"->groupid.toString, "auto.offset.reset"->"smallest"
    )
    val sparkOffsets: SparkOffsets= new SparkOffsets()
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val messages: InputDStream[(String, String)]= KafkaUtils.createDirectStream[String, String,StringDecoder,
      StringDecoder,(String,String)](ssc, kafkaParams,sparkOffsets.myFromOffsets(topics,groupid),messageHandler)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 先处理消息
        val lines = rdd.map(x=>{(x._1,x._2)})
        lines.foreach(println)
      }
    })
   // sparkOffsets.saveOffers(messages,groupid)
    ssc.start()
    ssc.awaitTermination()
  }
}

