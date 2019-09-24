package exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SparkSession}

/**
  *
  *    1、按照pois，分类businessarea，并统计每个businessarea的总数。
  */
object Test1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","G:\\BaiduNetdiskDownload")
    val sparkSession: SparkSession = SparkSession
      .builder ()
      .master ( "local" )
      .appName ( "Devicetype" )
      .config ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate ()
    sparkSession.sparkContext.setLogLevel("Error")
    val rdd: RDD[String] = sparkSession.sparkContext.textFile("D:\\lzxgit\\GP23_DMP\\data\\json.txt")

    var list: List[List[String]] = List()
    val MB= rdd.collect()

    for(i <- MB){
      val str: String = i.toString
      val jsonparse: JSONObject = JSON.parseObject(str)

      val status = jsonparse.getIntValue("status")
      if(status == 0) return ""
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null) return ""
      val poisArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null) return ""

      val res = collection.mutable.ListBuffer[String]()

      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          res.append(json.getString("businessarea"))
        }
      }
      val list1: List[String] = res.toList
      list:+=list1
    }
//    list.foreach(println)
//    list.flatMap(xml=>xml).foreach(println)

    val res1: List[(String, Int)] =
      list.flatMap(x => x)
      .filter(x => x != "[]")
        .map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size).toList.sortBy(x => x._2)

    res1.foreach(println)

  }
}
