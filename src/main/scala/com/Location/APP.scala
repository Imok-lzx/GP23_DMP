package com.Location




import com.util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 媒体分析指标
  */
class APP{

}
object APP {
  def main (args: Array[String]): Unit = {
    if(args.length!=3){
      println("目录输出错误")
      sys.exit()
    }
    System.setProperty("hadoop.home.dir","G:\\BaiduNetdiskDownload")
    val Array(inputPath,outputPath,txt)=args
    val sparkSession= SparkSession.builder ()
      .appName ( "ct" )
      .master ( "local" )
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate ()
    //读取数字字典
    val docMap = sparkSession.sparkContext.textFile(txt).map(_.split("\\s",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
//    进行广播
    val broadcast= sparkSession.sparkContext.broadcast(docMap)

//    读取数据文件
    val df = sparkSession.read.parquet(inputPath)
    val Rddapp: RDD[(String, List[Double])] = df.rdd.map ( row => {
      // 取媒体相关字段
      var appName = row.getAs [String]( "appname" )
      if (StringUtils.isBlank ( appName )) {
        appName = broadcast.value.getOrElse ( row.getAs [String]( "appid" ), "unknow" )
      }
      val requestmode = row.getAs [Int]( "requestmode" )
      val processnode = row.getAs [Int]( "processnode" )
      val iseffective = row.getAs [Int]( "iseffective" )
      val isbilling = row.getAs [Int]( "isbilling" )
      val isbid = row.getAs [Int]( "isbid" )
      val iswin = row.getAs [Int]( "iswin" )
      val adordeerid = row.getAs [Int]( "adorderid" )
      val winprice = row.getAs [Double]( "winprice" )
      val adpayment = row.getAs [Double]( "adpayment" )

      val rptList: List[Double] = RptUtils.ReqPt ( requestmode, processnode )
      val clickList: List[Double] = RptUtils.ClickPt ( requestmode, iseffective )
      val adList: List[Double] = RptUtils.AdPt ( iseffective, isbilling, isbid, iswin, adordeerid, winprice, adpayment )

      val allList: List[Double] = rptList ++ clickList ++ adList

      (appName, allList)
    } )
    Rddapp.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(x=>{
        x._1+x._2
      })
    }).map(xml=>{
      xml._1+","+xml._2.mkString(",")
    }).foreach(println)


  }
}
