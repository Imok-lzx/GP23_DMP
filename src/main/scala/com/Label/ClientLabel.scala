package com.Label

import com.util.LabelUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClientLabel {
  def main (args: Array[String]): Unit = {
    if (args.length != 2) {
      println ( "目录输入错误" )
      sys.exit ()
    }
    val Array ( inputPath, outputPath ) = args
    System.setProperty ( "hadoop.home.dir", "G:\\BaiduNetdiskDownload" )

    val sparkSession: SparkSession = SparkSession
      .builder ()
      .master ( "local" )
      .appName ( "appname" )
      .config ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate ()

    val df: DataFrame = sparkSession.read.parquet(inputPath)

    df.rdd.map(row=>{
      val client=row.getAs[Int]("client")
     LabelUtil.CaoZuo(client)
    }).foreach(println)
  }
}
