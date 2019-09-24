package com.graph_Test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例（好友关联推荐）
  * 位置D:\sparkoutput\parquet D:\sparkoutput\App G:\千峰学习\spark项目阶段\项目day01\Spark用户画像分析\
  */
object GraphTest {
  def main (args: Array[String]): Unit = {

    System.setProperty ( "hadoop.home.dir", "G:\\BaiduNetdiskDownload" )

    val sparkSession: SparkSession = SparkSession
      .builder ()
      .appName ( "graph" )
      .master ( "local[*]" )
      .getOrCreate ()

    sparkSession.sparkContext.setLogLevel("Error")
    //创建点和边
//    构建点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sparkSession.sparkContext.makeRDD ( Seq (
      (1L, ("小明", 26)),
      (2L, ("小红", 12)),
      (6L, ("小黑", 45)),
      (158L, ("小白", 26)),
      (9L, ("小绿", 14)),
      (7L, ("小爱的", 7)),
      (133L, ("小密", 29)),
      (138L, ("小主线程", 54)),
      (21L, ("小好", 22)),
      (5L, ("小有", 26)),
      (16L, ("小i", 15)),
      (44L, ("小是", 1))
    ) )
   //边的集合
    val edgeRDD: RDD[Edge[Int]] = sparkSession.sparkContext.makeRDD ( Seq (
      Edge ( 1L, 133L, 0 ),
      Edge ( 2L, 133L, 0 ),
      Edge ( 6L, 133L, 0 ),
      Edge ( 9L, 133L, 0 ),
      Edge ( 6L, 138L, 0 ),
      Edge ( 16L, 138L, 0 ),
      Edge ( 21L, 138L, 0 ),
      Edge ( 44L, 138L, 0 ),
      Edge ( 5L, 158L, 0 ),
      Edge ( 7L, 158L, 0 )

    ) )


    //构建图
    val graph = Graph( vertexRDD,edgeRDD)
    //取顶点Id
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
//      匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }
      .reduceByKey(_++_).foreach(println)

  }
}
