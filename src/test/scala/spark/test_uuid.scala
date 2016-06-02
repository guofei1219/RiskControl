package spark

import java.util.UUID

/**
  * Created by 郭飞 on 2016/5/31.
  */
object test_uuid {
  def main(args: Array[String]) {
    val uid = UUID.randomUUID()
    System.out.println(uid)
  }
}
