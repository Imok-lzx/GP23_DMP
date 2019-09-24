package com.Location

import org.apache.spark.sql.{DataFrame, SparkSession}

object NetWork {
  def main (args: Array[String]): Unit = {
    if (args.length!=2){
      println("目录输入错误")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args
    System.setProperty("hadoop.home.dir","G:\\BaiduNetdiskDownload")
    val sparkSession: SparkSession = SparkSession
      .builder ()
      .appName ( "NetWork" )
      .master ( "local" )
      .config ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate ()
    val df: DataFrame = sparkSession.read.parquet(inputPath)

    df.createTempView("network")
    val sql ="select " +
      "networkmannername," +
      "sum(one) requestmode ," +
      "sum(two) processnode ," +
      "sum(three) iseffective," +
      "sum(four) isbilling," +
      "sum(five) isbid," +
      "1.0*sum(five)/sum(four) isbidAV," +
      "sum(six) iswin," +
      "sum(seven) adordeerid," +
      "1.0*sum(seven)/count(seven) adordeeridAV," +
      "sum(nine) winprice," +
      "sum(eight) adpayment" +
      " from" +
      "("+
      "select " +
      "networkmannername," +
      "(case when requestmode=1 and processnode>=1 then 1 else 0 end) one," +
      "(case when requestmode=1 and processnode>=2 then 1 else 0 end) two," +
      "(case when requestmode=1 and processnode=3 then 1 else 0 end) three," +
      "(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) four," +
      "(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) five," +
      "(case when requestmode=2 and iseffective=1 then 1 else 0 end) six," +
      "(case when requestmode=3 and iseffective=1 then 1 else 0 end) seven," +
      "(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) eight," +
      "(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) nine " +
      " from network"+
      ") tmp " +
      "group by networkmannername " +
      "order by networkmannername"

    val df2: DataFrame = sparkSession.sql(sql)
     df2.write.parquet(outputPath)


  }
}
