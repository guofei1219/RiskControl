/*
package spark

import java.util.Properties

import kafka.consumer.{ConsumerConfig, ConsumerConnector, ConsumerIterator, KafkaStream}

/**
  * Created by 郭飞 on 2016/5/30.
  */
object KafkaInput {
  	private val TOPIC: String = "test"
    def main(args: Array[String]){
      val props: Properties = new Properties
      props.put("zookeeper.connect", "hadoop1:2181,hadoop2:2181,hadoop3:2181")
      props.put("group.id", "testGroup")
      props.put("zookeeper.session.timeout.ms", "400")
      props.put("zookeeper.sync.time.ms", "200")
      props.put("auto.commit.interval.ms", "1000")
      val consumer = new ConsumerConfig(props)
      val topicCountMap  = Map[String, Integer](
        TOPIC->new Integer(1)
      )
      val consumerMap = consumer.createMessageStreams(topicCountMap)
      val stream = consumerMap.get(KafkaConsumerSimple.TOPIC).get(0)
      val it = stream.iterator
      while (it.hasNext) {
      {
        System.out.println(new String(it.next.message))
      }
      }
    }

}
*/
