package kafka2streaming

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable

/**
  * Created by hui on 2016/6/3.
  */
class SparkOffsets extends  Serializable{


  val logger:Logger = LoggerFactory.getLogger("SparkOffsets")

  val zkClient:ZkClient = new ZkClient("hc4:2181",30000, 30000,ZKStringSerializer)

  def myFromOffsets(topic:String,groupid:String):Map[TopicAndPartition, Long]={

    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    //得到指定topic的所有分区
    val topic2Partitions = ZkUtils.getPartitionsForTopics(zkClient,topic.split(",").toSeq)
    println("得到了指定topic的所有分区")
    topic2Partitions.foreach(topic2Partitions => {
      val topic:String = topic2Partitions._1 //topic
      val partitions:Seq[Int] = topic2Partitions._2 //分区
      val topicDirs = new ZKGroupTopicDirs(groupid, topic)

      partitions.foreach(partition => {//遍历每一个分区
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"//拼接每一个分区的offerSet的路径
        ZkUtils.makeSurePersistentPathExists(zkClient, zkPath)//持久路径是否存在，不存在就创建一个
        val untilOffset = zkClient.readData[String](zkPath)//读取offset数据

        val tp = TopicAndPartition(topic, partition)
        val offset = try {
          if (untilOffset == null || untilOffset.trim == "")//没有取到offset数据。
            getMaxOffset(tp)
          else
            untilOffset.toLong
        } catch {
          case e: Exception => getMaxOffset(tp)
        }
        fromOffsets += (tp -> offset)
        logger.info(s"Offset init: set offset of $topic/$partition as $offset")

      })
    })
    fromOffsets
  }

  /**
    * 第一次启动spark任务或者zookeeper上的数据被删除或设置出错时，将选取最大的offset开始消费。
    * @param tp
    * @return
    */
  private def getMaxOffset(tp:TopicAndPartition):Long = {
    val request = OffsetRequest(immutable.Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))

    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match { //得到指定topic的活跃分区（leader）
      case Some(brokerId) => {
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
          case Some(brokerInfoString) => {
            Json.parseFull(brokerInfoString) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                new SimpleConsumer(host, port, 10000, 100000, "getMaxOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                  .offsets
                  .head
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
            }
          }
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      }
      case None =>
        throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
    }
  }

  def saveOffers(messages:InputDStream[(String, String)],groupId:String)={
    var offsetRanges = Array[OffsetRange]()
    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      offsetRanges.foreach(o => {
        val topicDirs = new ZKGroupTopicDirs(groupId, o.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
        logger.info(s"Offset update: set offset of ${o.topic}/${o.partition} as ${o.untilOffset.toString}")
      })
    })
  }
}