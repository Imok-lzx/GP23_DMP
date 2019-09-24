package com.Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 统计省市指标
  */
object Procity {
  def main (args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","G:\\BaiduNetdiskDownload")
    if (args.length !=1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath) = args
    val sparkSession: SparkSession = SparkSession.builder ().appName ( "ct" )
      .master ( "local" )
      .config ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate ()

    //获取数据
    val df: DataFrame = sparkSession.read.parquet(inputPath)
    //注册临时视图
    df.createTempView("log")
    val df2: DataFrame = sparkSession.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname ")
//    df2.write.partitionBy("provincename","cityname").json("D:\\procity")

    sparkSession.sql("select*From log").show()

////    2存储到mysql
////    通过config配置文件依赖进行加载相关的配置信息
//    val load= ConfigFactory.load()
//    //创建Properties 对象
//    val prop=new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
////    存储
//    df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),prop)
//
//


    sparkSession.stop()


  }
}
