package com.Label

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object AppName {
  def main (args: Array[String]): Unit = {
    if (args.length !=3){
      println("目录输入错误")
      sys.exit()
    }
    val Array(inputPath,outputPath,docs)=args
    System.setProperty("hadoop.home.dir","G:\\BaiduNetdiskDownload")

    val sparkSession: SparkSession = SparkSession
      .builder ()
      .master ( "local" )
      .appName ( "appname" )
      .config ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate ()

    val docsMap= sparkSession.sparkContext.textFile ( docs ).map ( _.split ( "\\s", -1 ) )
      .filter ( _.length >= 5 ).map ( arr => (arr ( 4 ), arr ( 1 )) ).collectAsMap ()

    val broadcast = sparkSession.sparkContext.broadcast(docsMap)

    val df: DataFrame = sparkSession.read.parquet(inputPath)

    df.rdd.map(row=>{

      var appname = row.getAs[String]("appname")
      if (StringUtils.isBlank(appname)){
       appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"123456782345672345672345678456784567")
      }

      Map(("APP"+""+appname,1))
    }).foreach(println)
  }
}
