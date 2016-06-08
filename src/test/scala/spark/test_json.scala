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

    //mysql配置表
    val colConf = Map(
      "hb_gd_br_special_list" ->"id_card_qry_bank_bad	id_card_qry_bank_ovdue	id_card_qry_bank_fraud	id_card_qry_bank_rej	id_card_qry_loan_p2p_bad	id_card_qry_loan_p2p_ovdue	id_card_qry_loan_p2p_fraud	id_card_qry_loan_p2p_rej	id_card_qry_telecom_ovdue	id_card_qry_court_bad	id_card_qry_court_executed	id_card_qry_p2p_bad	id_card_qry_p2p_ovdue	id_card_qry_p2p_fraud	id_card_qry_p2p_rej	id_card_qry_loan_bad	id_card_qry_loan_ovdue	id_card_qry_loan_fraud	id_card_qry_loan_ref	id_card_qry_consm_fin_bad	id_card_qry_consm_fin_ovdue	id_card_qry_consm_fin_fraud	id_card_qry_consm_fin_ref	id_card_qry_nbank_other_bad	id_card_qry_nbank_other_ovdue	id_card_qry_nbank_other_fraud	id_card_qry_nbank_other_ref	lkman_mobile_qry_bank_bad	lkman_mobile_qry_bank_ovdue	lkman_mobile_qry_bank_fraud	lkman_mobile_qry_bank_ref	lkman_mobile_qry_telecom_ovdue	lkman_mobile_qry_p2p_bad	lkman_mobile_qry_p2p_ovdue	lkman_mobile_qry_p2p_fraud	lkman_mobile_qry_p2p_ref	lkman_mobile_qry_loan_bad	lkman_mobile_qry_loan_ovdue	lkman_mobile_qry_loan_fraud	lkman_mobile_qry_loan_ref	lkman_mobile_qry_consm_fin_bad	lkman_mobile_qry_consm_fin_ovdue	lkman_mobile_qry_consm_fin_fraud	lkman_mobile_qry_consm_fin_ref	lkman_mobile_qry_nbank_other_bad	lkman_mobile_qry_nbank_other_ovdue	lkman_mobile_qry_nbank_other_fraud	lkman_mobile_qry_nbank_other_ref	mobile_qry_bank_bad	mobile_qry_bank_ovdue	mobile_qry_bank_fraud	mobile_qry_bank_ref	mobile_qry_loan_p2p_bad	mobile_qry_loan_p2p_ovdue	mobile_qry_loan_p2p_fraud	mobile_qry_loan_p2p_ref	mobile_qry_telecom_ovdue	mobile_qry_p2p_bad	mobile_qry_p2p_ovdue	mobile_qry_p2p_fraud	mobile_qry_p2p_ref	mobile_qry_loan_bad	mobile_qry_loan_ovdue	mobile_qry_loan_fraud	mobile_qry_loan_ref	mobile_qry_consm_fin_bad	mobile_qry_consm_fin_ovdue	mobile_qry_consm_fin_fraud	mobile_qry_consm_fin_ref	mobile_qry_nbank_other_bad	mobile_qry_nbank_other_ovdue	mobile_qry_nbank_other_fraud	mobile_qry_nbank_other_ref	bairong_id_qry_bank_bad	bairong_id_qry_bank_ovdue	bairong_id_qry_bank_fraud	bairong_id_qry_bank_ref	bairong_id_qry_loan_p2p_bad	bairong_id_qry_loan_p2p_ovdue	bairong_id_qry_loan_p2p_fraud	bairong_id_qry_loan_p2p_ref	bairong_id_qry_telecom_ovdue	bairong_user_gid_qry_p2p_bad	bairong_user_gid_qry_p2p_ovdue	bairong_user_gid_qry_p2p_fraud	bairong_user_gid_qry_p2p_ref	bairong_user_gid_qry_loan_bad	bairong_user_gid_qry_loan_ovdue	bairong_user_gid_qry_loan_fraud	bairong_user_gid_qry_loan_ref	bairong_user_gid_qry_consm_fin_bad	bairong_user_gid_qry_consm_fin_ovdue	bairong_user_gid_qry_consm_fin_fraud	bairong_user_gid_qry_consm_fin_ref	bairong_user_gid_qry_nbank_other_bad	bairong_user_gid_qry_nbank_other_ovdue	bairong_user_gid_qry_nbank_other_fraud	bairong_user_gid_qry_nbank_other_ref	"
      ,
      "hb_gd_br_apply_loan" ->"la_m3_bairong_id_bank_selfnum	la_m3_bairong_id_bairong_bank_allnum	la_m3_bairong_id_bairong_bank_orgnum	la_m3_bairong_id_notbank_selfnum	la_m3_bairong_id_bairong_notbank_allnum	la_m3_bairong_id_bairong_notbank_orgnum	la_m3_id_card_bank_selfnum	la_m3_id_card_bank_allnum	la_m3_id_card_bank_orgnum	la_m3_id_card_notbank_selfnum	la_m3_id_card_notbank_allnum	la_m3_id_card_notbank_orgnum	la_m3_mobile_bank_selfnum	la_m3_mobile_bank_allnum	la_m3_mobile_bank_orgnum	la_m3_mobile_notbank_selfnum	la_m3_mobile_notbank_allnum	la_m3_mobile_notbank_orgnum	la_m6_bairong_id_bank_selfnum	la_m6_bairong_id_bairong_bank_allnum	la_m6_bairong_id_bairong_bank_orgnum	la_m6_bairong_id_notbank_selfnum	la_m6_bairong_id_bairong_notbank_allnum	la_m6_bairong_id_bairong_notbank_orgnum	la_m6_id_card_bank_selfnum	la_m6_id_card_bank_allnum	la_m6_id_card_bank_orgnum	la_m6_id_card_notbank_selfnum	la_m6_id_card_notbank_allnum	la_m6_id_card_notbank_orgnum	la_m6_mobile_bank_selfnum	la_m6_mobile_bank_allnum	la_m6_mobile_bank_orgnum	la_m6_mobile_notbank_selfnum	la_m6_mobile_notbank_allnum	la_m6_mobile_notbank_orgnum	la_m12_bairong_id_bank_selfnum	la_m12_bairong_id_bairong_bank_allnum	la_m12_bairong_id_bairong_bank_orgnum	la_m12_bairong_id_notbank_selfnum	la_m12_bairong_id_bairong_notbank_allnum	la_m12_bairong_id_bairong_notbank_orgnum	la_m12_id_card_bank_selfnum	la_m12_id_card_bank_allnum	la_m12_id_card_bank_orgnum	la_m12_id_card_notbank_selfnum	la_m12_id_card_notbank_allnum	la_m12_id_card_notbank_orgnum	la_m12_mobile_bank_selfnum	la_m12_mobile_bank_allnum	la_m12_mobile_bank_orgnum	la_m12_mobile_notbank_selfnum	la_m12_mobile_notbank_allnum	la_m12_mobile_notbank_orgnum	"
    )

    //kafka消息
    val kafka_CH0001_R004 =
      "{\n\t\"" +
      "hb_gd_br_apply_loan\":\"U0DqUIpmsATrhNyiijHcJsoojlNjN7LL^郑张孙^330325197204275228^13706887287^^^^^^^0^0^0^0^2^2^0^0^0^0^2^2^^^^^^^0^1^1^0^2^2^0^1^1^0^2^2^^^^^^^0^2^1^0^2^2^0^2^1^0^2^2^2016-06-07 13:50:00\",\n\t\"" +
      "hb_gd_br_special_list\":\"U0DqUIpmsATrhNyiijHcJsoojlNjN7LL^郑张孙^330325197204275228^13706887287^^^0^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^0^0^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^2016-06-07 13:50:00\"\n" +
      "}"

    //val str5= "{\"prod1\":\"value1^value2^value3^value4\"\n ,\"prod2\":\"value1^value2^value3^value4\"\n ,\"prod3\":\"value1^value2^value3^value40x0dvalue5^value6^value7^value80x0dvalue9^value10^value11^value12\"\n}"
    //解析Kafka json数据为Map
    val b = JSON.parseFull(kafka_CH0001_R004)
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => {
        //拼接返回JSON串缓存
        val buffer = new StringBuffer()
        var values_buffer = new StringBuffer()
        buffer.append("{\n")
        buffer.append("\"message\":\"查询成功\",\n")
        // "data":{
        buffer.append("\"data\":\"{\"\n")
        //遍历json数据
        map.keys.foreach(jsonKey=>{
          //拆分json一对多情况
          if(map(jsonKey).toString.contains("0x0d")){
            val values = map(jsonKey).toString.split("0x0d")

            //System.out.println(values.length)
            values_buffer.append("\"  "+jsonKey+"\""+":["+"\n")
            for(v <-  values){
              values_buffer.append(
                "           {\n                " +
                "\""+jsonKey+"\""+":\""+v+"\""+",\n           " +

                "},\n")

            }
            //values_buffer = values_buffer.toString.substring(0,values_buffer.length()-1)
            values_buffer.append("        ]")
          }else{
            //json一对一情况
            /**
              * 比对Map中key与JSON中key是否相同，相同的话拼接新的JSON串
              * 第一步：遍历Map，找出对应Key的 value值并转换为数组
              * 第二步：拆分JSON对应key的value值并转换为数组
              * 第三步：双重For循环遍历一二步集合拼接JSON
              */
            colConf.keys.foreach(MapKey=>{
              if(jsonKey.equals(MapKey)){
                /**
                  * 第一步
                  */
                //拿到Map对应key的value值
                val mapValue = colConf(MapKey)
                //MapValue转换为数组
                val mapArray = mapValue.split("\t")

                /**
                  * 第二步
                  */
                val jsonArray = map(jsonKey).toString.split("\\^")

                /**
                  * 第三步
                  */
                buffer.append(" \""+jsonKey+"\""+":{"+"\n                ")
                for ( i <- 0 to (mapArray.length - 1)) {

                  buffer.append("\""+mapArray(i)+"\""+":\""+jsonArray(i+4)+"\""+",\n        ")

                }
                buffer.append("}\n")
              }
            })

          }
        })
        buffer.append("   }\n")

/*        “batchSeqNum”:”123123719321230812301023”,
        "errorcode":"000000"
        buffer.append("\"batchSeqNum\":"+"\""+ jsonArray(g) +"\""")*/
        buffer.append(values_buffer)
        buffer.append("\n}")
        println(buffer)
      }
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
  }
}

/*"la_m3_bairong_id_bank_selfnum	la_m3_bairong_id_bairong_bank_allnum	" +
      "la_m3_bairong_id_bairong_bank_orgnum	la_m3_bairong_id_notbank_selfnum	" +
      "la_m3_bairong_id_bairong_notbank_allnum	la_m3_bairong_id_bairong_notbank_orgnum	" +
      "la_m3_id_card_bank_selfnum	la_m3_id_card_bank_allnum	la_m3_id_card_bank_orgnum	" +
      "la_m3_id_card_notbank_selfnum	la_m3_id_card_notbank_allnum	la_m3_id_card_notbank_orgnum	" +
      "la_m3_mobile_bank_selfnum	la_m3_mobile_bank_allnum	la_m3_mobile_bank_orgnum	" +
      "la_m3_mobile_notbank_selfnum	la_m3_mobile_notbank_allnum	la_m3_mobile_notbank_orgnum	" +
      "la_m6_bairong_id_bank_selfnum	la_m6_bairong_id_bairong_bank_allnum	la_m6_bairong_id_bairong_bank_orgnum	" +
      "la_m6_bairong_id_notbank_selfnum	la_m6_bairong_id_bairong_notbank_allnum	" +
      "la_m6_bairong_id_bairong_notbank_orgnum	la_m6_id_card_bank_selfnum	la_m6_id_card_bank_allnum	" +
      "la_m6_id_card_bank_orgnum	la_m6_id_card_notbank_selfnum	la_m6_id_card_notbank_allnum	" +
      "la_m6_id_card_notbank_orgnum	la_m6_mobile_bank_selfnum	la_m6_mobile_bank_allnum	la_m6_mobile_bank_orgnum	" +
      "la_m6_mobile_notbank_selfnum	la_m6_mobile_notbank_allnum	la_m6_mobile_notbank_orgnum	" +
      "la_m12_bairong_id_bank_selfnum	la_m12_bairong_id_bairong_bank_allnum	la_m12_bairong_id_bairong_bank_orgnum	" +
      "la_m12_bairong_id_notbank_selfnum	la_m12_bairong_id_bairong_notbank_allnum	" +
      "la_m12_bairong_id_bairong_notbank_orgnum	la_m12_id_card_bank_selfnum	la_m12_id_card_bank_allnum	" +
      "la_m12_id_card_bank_orgnum	la_m12_id_card_notbank_selfnum	la_m12_id_card_notbank_allnum	" +
      "la_m12_id_card_notbank_orgnum	la_m12_mobile_bank_selfnum	la_m12_mobile_bank_allnum	" +
      "la_m12_mobile_bank_orgnum	la_m12_mobile_notbank_selfnum	la_m12_mobile_notbank_allnum	" +
      "la_m12_mobile_notbank_orgnum	"*/

/*

"id_card_qry_bank_bad	id_card_qry_bank_ovdue	id_card_qry_bank_fraud	" +
"id_card_qry_bank_rej	id_card_qry_loan_p2p_bad	id_card_qry_loan_p2p_ovdue	" +
"id_card_qry_loan_p2p_fraud	id_card_qry_loan_p2p_rej	id_card_qry_telecom_ovdue	" +
"id_card_qry_court_bad	id_card_qry_court_executed	id_card_qry_p2p_bad	" +
"id_card_qry_p2p_ovdue	id_card_qry_p2p_fraud	id_card_qry_p2p_rej	" +
"id_card_qry_loan_bad	id_card_qry_loan_ovdue	id_card_qry_loan_fraud	" +
"id_card_qry_loan_ref	id_card_qry_consm_fin_bad	id_card_qry_consm_fin_ovdue	" +
"id_card_qry_consm_fin_fraud	id_card_qry_consm_fin_ref	id_card_qry_nbank_other_bad	" +
"id_card_qry_nbank_other_ovdue	id_card_qry_nbank_other_fraud	id_card_qry_nbank_other_ref	" +
"lkman_mobile_qry_bank_bad	lkman_mobile_qry_bank_ovdue	lkman_mobile_qry_bank_fraud	" +
"lkman_mobile_qry_bank_ref	lkman_mobile_qry_telecom_ovdue	lkman_mobile_qry_p2p_bad	" +
"lkman_mobile_qry_p2p_ovdue	lkman_mobile_qry_p2p_fraud	lkman_mobile_qry_p2p_ref	" +
"lkman_mobile_qry_loan_bad	lkman_mobile_qry_loan_ovdue	lkman_mobile_qry_loan_fraud	" +
"lkman_mobile_qry_loan_ref	lkman_mobile_qry_consm_fin_bad	lkman_mobile_qry_consm_fin_ovdue	" +
"lkman_mobile_qry_consm_fin_fraud	lkman_mobile_qry_consm_fin_ref	lkman_mobile_qry_nbank_other_bad	" +
"lkman_mobile_qry_nbank_other_ovdue	lkman_mobile_qry_nbank_other_fraud	lkman_mobile_qry_nbank_other_ref	" +
"mobile_qry_bank_bad	mobile_qry_bank_ovdue	mobile_qry_bank_fraud	" +
"mobile_qry_bank_ref	mobile_qry_loan_p2p_bad	mobile_qry_loan_p2p_ovdue	" +
"mobile_qry_loan_p2p_fraud	mobile_qry_loan_p2p_ref	mobile_qry_telecom_ovdue	" +
"mobile_qry_p2p_bad	mobile_qry_p2p_ovdue	mobile_qry_p2p_fraud	" +
"mobile_qry_p2p_ref	mobile_qry_loan_bad	mobile_qry_loan_ovdue	" +
"mobile_qry_loan_fraud	mobile_qry_loan_ref	mobile_qry_consm_fin_bad	" +
"mobile_qry_consm_fin_ovdue	mobile_qry_consm_fin_fraud	mobile_qry_consm_fin_ref	" +
"mobile_qry_nbank_other_bad	mobile_qry_nbank_other_ovdue	mobile_qry_nbank_other_fraud	" +
"mobile_qry_nbank_other_ref	bairong_id_qry_bank_bad	bairong_id_qry_bank_ovdue	" +
"bairong_id_qry_bank_fraud	bairong_id_qry_bank_ref	bairong_id_qry_loan_p2p_bad	" +
"bairong_id_qry_loan_p2p_ovdue	bairong_id_qry_loan_p2p_fraud	bairong_id_qry_loan_p2p_ref	" +
"bairong_id_qry_telecom_ovdue	bairong_user_gid_qry_p2p_bad	bairong_user_gid_qry_p2p_ovdue	" +
"bairong_user_gid_qry_p2p_fraud	bairong_user_gid_qry_p2p_ref	bairong_user_gid_qry_loan_bad	" +
"bairong_user_gid_qry_loan_ovdue	bairong_user_gid_qry_loan_fraud	bairong_user_gid_qry_loan_ref	" +
"bairong_user_gid_qry_consm_fin_bad	bairong_user_gid_qry_consm_fin_ovdue	bairong_user_gid_qry_consm_fin_fraud	" +
"bairong_user_gid_qry_consm_fin_ref	bairong_user_gid_qry_nbank_other_bad	bairong_user_gid_qry_nbank_other_ovdue  bairong_user_gid_qry_nbank_other_fraud	bairong_user_gid_qry_nbank_other_ref"*/
