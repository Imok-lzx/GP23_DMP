package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AdIdTag extends Tag{
  override def makeTags (args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    // 获取数据类型
    val row: Row = args(0).asInstanceOf[Row]
    //获取渠道id
    val adid: Int = row.getAs[Int]("adplatformproviderid")
if (adid != null){
    list:+=("CN"+adid,1)
}
    list

  }
}
