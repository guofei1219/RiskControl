package spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by 郭飞 on 2016/5/25.
  */
object test_join {
  def main(args: Array[String]) {
/*
    val str5= "{\"prod1\":\"value1^value2^value3^value4\"\n ," +
      "\"prod2\":\"value1^value2^value3^value4\"\n ," +
      "\"prod3\":\"value1^value2^value3^value40x0dvalue5^value6^value7^value80x0dvalue9^value10^value11^value12\"\n}"

*/

    val kafka = Map("prod1" -> "value1^value2^value3^value4",
                     "prod2" -> "value1^value2^value3^value4")

    val Kafka_RDD = kafka.keys.foreach{ i =>
      (i,kafka(i))
      print( "Key = " + i )
      println(" Value = " + kafka(i) )
    }
    val conf = new SparkConf().setAppName("join").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val table1 = sc.parallelize(List("prod1&value1^value2^value3^value4",
      "prod2&value1^value2^value3^value4"))
    val rdd1 = table1.map(x=>{
      val line = x.split("&")
      val table = line(0)
      val value = line(1)
      (table,value)
    })

    val table2 = sc.parallelize(List("prod1&value1^value2^value3^value4",
      "prod2&value1^value2^value3^value4"))
    val rdd2 = table2.map(x=>{
      val line = x.split("&")
      val table = line(0)
      val value = line(1)
      (table,value)
    })

    val result = rdd1.join(rdd2).saveAsTextFile("file:///C://work//join.txt")
    System.out.println(result)






  }
}
