package spark


import org.codehaus.jettison.json.JSONObject

import scala.util.parsing.json.JSON


/**
  * Created by 郭飞 on 2016/5/24.
  */
object test_json {
  def main(args: Array[String]) {
    val str4 = "{"+
                "\"prod1\":\"value1^value2^value3^value4\","+
                "\"prod3\":\"value1^value2^value3^value40x0dvalue5^value6^value7^value80x0dvalue9^value10^value11^value12\","+
                "\"prod2\":\"value1^value2^value3^value4\""+
                "}"

    //val str5= "{\"prod1\":\"value1^value2^value3^value4\"\n ,\"prod2\":\"value1^value2^value3^value4\"\n ,\"prod3\":\"value1^value2^value3^value40x0dvalue5^value6^value7^value80x0dvalue9^value10^value11^value12\"\n}"
    //解析Kafka json数据为Map
    val b = JSON.parseFull(str4)
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => {
        //拼接返回JSON串缓存
        val buffer = new StringBuffer()
        var values_buffer = new StringBuffer()
        buffer.append("{\n")
        buffer.append("\"message\":\"查询成功\",\n")
        //遍历json数据
        map.keys.foreach(x=>{
          //拆分json一对多情况
          if(map(x).toString.contains("0x0d")){
            val values = map(x).toString.split("0x0d")

            //System.out.println(values.length)
            values_buffer.append("\""+x+"\""+":["+"\n")
            for(v <-  values){
              values_buffer.append(
                "           {\n                " +
                "\""+x+"\""+":\""+v+"\""+",\n           " +

                "},\n")

            }
            //values_buffer = values_buffer.toString.substring(0,values_buffer.length()-1)
            values_buffer.append("        ]")
          }else{
            //json一对一情况
           // println("键："+x+" 值："+map(x))
            buffer.append("\""+x+"\""+":{"+"\n                " +

              "\""+x+"\""+":\""+map(x)+"\""+",\n        " +

              "}\n")
          }
        })
        buffer.append(values_buffer)
        buffer.append("\n}")
        println(buffer)
      }
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
  }
}
