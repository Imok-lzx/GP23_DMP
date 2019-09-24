package com.Location

import com.util.RptUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object Devicetype {
  def main (args: Array[String]): Unit = {

    if(args.length!=2){
      println("目录输入不正确")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args
    System.setProperty("hadoop.home.dir","G:\\BaiduNetdiskDownload")

    val sparkSession: SparkSession = SparkSession
      .builder ()
      .master ( "local" )
      .appName ( "Devicetype" )
      .config ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate ()
    val df: DataFrame = sparkSession.read.parquet(inputPath)
    df.rdd.map(row=>{
      //根据指标的字段获取数据
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 处理展示点击
      val clickList = RptUtils.ClickPt(requestmode,iseffective)
      // 处理广告
      val adList = RptUtils.AdPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 所有指标
      val allList:List[Double] = rptList ++ clickList ++ adList
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(xml=>{xml._1+xml._2})
    }).map(m=>m._1+","+ m._2.mkString(","))
      .foreach(println)
//      .saveAsTextFile(outputPath)
  }
}
