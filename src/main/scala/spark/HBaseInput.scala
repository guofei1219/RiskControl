package spark

import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Scan}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.spark._

import scala.collection.JavaConverters._
/**
  * Created by 郭飞 on 2016/5/26.
  */
object HBaseInput {
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

    //Spark整合HBase
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //keyValue is a RDD[java.util.list[hbase.KeyValue]]
    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    //outPut is a RDD[String], in which each line represents a record in HBase
    val outPut = keyValue.flatMap(x =>  x.asScala.map(cell =>
      "columnFamily=%s,qualifier=%s,timestamp=%s,type=%s,value=%s".format(
        Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
        Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
        cell.getTimestamp.toString,
        Type.codeToType(cell.getTypeByte),
        Bytes.toStringBinary(CellUtil.cloneValue(cell))
      )
    )
    )
    outPut.foreach(println)

    /*
     单行读数据
     1）初始化HTable实例
     2）构造实体类Get ,Get类封装所需要的行键  列族  列名
     3）执行查询打印结果
    */
    val hTable = new HTable(hbaseConf ,Bytes.toBytes("table1"))
    val row = new Get(Bytes.toBytes("guofei"))
    row.addColumn(Bytes.toBytes("col1"),Bytes.toBytes("a"))

    val dbResult = hTable.get(row).list()
    //System.out.println(Bytes.toString(dbResult.get(0).getValue))


    /*
      扫描读
      1）初始化HTbable实例
      2）构造实体类 Scan，Scan类封装所需的列族 列名和其他的属性设置
      3）执行查询并输出结果
     */
    val scanner = new Scan()
    scanner.setBatch(0)
    scanner.setCaching(1000)
    val rsScanner = hTable.getScanner(scanner)
    System.out.println(rsScanner)
    /*    for(res <- rsScanner){
          res
        }*/

    sc.stop()
  }
}