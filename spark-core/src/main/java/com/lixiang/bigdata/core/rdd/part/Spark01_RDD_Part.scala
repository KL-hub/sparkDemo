package com.lixiang.bigdata.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val rdd = sc.makeRDD(List(
      ("nba","12345678"),
      ("cba","12345678"),
      ("wba","12345678"),
      ("wba","12345678"),
    ),3)

    rdd.partitionBy(new MyPartitioner)
    rdd.saveAsTextFile("outpath")

    //TODO
    //所谓的行动算子，其实就是触发job的执行
    rdd.collect();
    sc.stop()
  }

  /**
   * 自定义分区器
   */
  class MyPartitioner extends Partitioner{
    //分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值 返回数据的分区索引(从 0开始)
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "cba"=>1
        case _=>2
      }
//       if(key=="nba"){
//         0
//       }else if( key =="wnba"){
//         1
//       }else if (key =="cba"){
//         2
//       }else{
//         2
//       }

    }
  }
}
