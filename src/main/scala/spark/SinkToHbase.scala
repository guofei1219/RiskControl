package spark

import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 郭飞 on 2016/5/26.
  */
object SinkToHbase {
  def main(args: Array[String]) {
    //初始化spark配置
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]")
    //spark入口类
    val sc = new SparkContext(sparkConf)
    //创建HBase配置
    val hbaseConf = HBaseConfiguration.create()
    //设置HBase使用的Zookeeper地址（后续抽取到xml配置文件）
    val zk = "192.168.20.131"
    //设置要查询的表名（后续抽取到xml配置文件）hb_trdpty_json_result
    val tableName = "table1"
    // Other options for hbase configuration are available, please check
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HConstants.html
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zk)
    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    /*
      插入一行数据
     */
    val p = new Put(Bytes.toBytes("guofei")) //行键
    p.add("col1".getBytes, "a".getBytes, Bytes.toBytes("guofei1"))//三个参数分别为列簇 列名 列值
    val myTable = new HTable(hbaseConf, TableName.valueOf(tableName))
    myTable.setAutoFlush(false, false)
    myTable.setWriteBufferSize(3*1024*1024)
    // myTable.put(p)
    //myTable.flushCommits()




    sc.stop()
  }
}
