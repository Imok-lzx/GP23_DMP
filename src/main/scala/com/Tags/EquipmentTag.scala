package com.Tags

import com.util.Tag
import org.apache.spark.sql.Row

object EquipmentTag extends Tag{
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list=List[(String, Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val client: Int = row.getAs[Int]("client")
    val networkname: String = row.getAs[String]("networkmannername")
    val ispname:String = row.getAs[String]("ispname")
    client match {
      case 1 => list:+=("AndroidD00010001",1)
      case 2 => list:+=("IOSD00010002",1)
      case 3 => list:+=("WinPhoneD00010003",1)
      case _ => list:+=("其他D00010004",1)
    }

    networkname match {
      case "WIFI" => list:+=(networkname+"D00020001",1)
      case "4G"=> list:+=(networkname+"D00020002",1)
      case "3G"=> list:+=(networkname+"D00020003",1)
      case "2G"=> list:+=(networkname+"D00020004",1)
      case _ => list:+=("_ D00020005",1)
    }

    ispname match {
      case "移动" => list:+=(ispname+"D00030001 ",1)
      case "联通" => list:+=(ispname+"D00030002 ",1)
      case "电信" => list:+=(ispname+"D00030003 ",1)
      case _ => list:+=("其他D00030004",1)
    }

    list


  }
}
