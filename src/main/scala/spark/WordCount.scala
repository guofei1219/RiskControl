package spark

import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by 郭飞 on 2016/5/23.
  */
object WordCount {
  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称及Master地址
    val conf = new SparkConf().setAppName("WC").setMaster("local")
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    //使用sc创建RDD并执行相应的transformation和action
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile(args(1))
    val result = sc.textFile("hdfs://hadoop1:9000/input/word/word.txt").
      flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKey(_+_, 1).
      sortBy(_._2, false)

    result.foreach(x=>{
      val word = x._1
      val num = x._2
      println(word+" "+num )
    })
    result.saveAsTextFile("C://work//result")

    //停止sc，结束该任务
    sc.stop()
  }
}
