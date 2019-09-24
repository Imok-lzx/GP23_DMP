package com.util

import com.Tags.BusinessTag
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test {

    def main(args: Array[String]): Unit = {
      System.setProperty("hadoop.home.dir", "G:\\BaiduNetdiskDownload")
      val spark = SparkSession.builder().appName("Tags").master("local[*]").getOrCreate()
      import spark.implicits._

      // 读取数据文件
      val df = spark.read.parquet("D:\\sparkoutput\\par")
      df.map(row=>{
        // 圈
        val businessList = BusinessTag.makeTags(row)
//          AmapUtil.getBusinessFromAmap(
//          String2Type.toDouble(row.getAs[String]("lat")),
//          String2Type.toDouble(row.getAs[String]("long")))

        businessList
      }).rdd.foreach(println)


//    val str: String = AmapUtil.getBusinessFromAmap(116.0,39.0)
//    println(str)

  }
}
