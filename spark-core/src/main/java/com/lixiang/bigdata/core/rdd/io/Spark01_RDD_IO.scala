package com.lixiang.bigdata.core.rdd.io

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark01_RDD_IO {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val rdd = sc.makeRDD(List(
      ("nba","12345678"),
      ("cba","12345678"),
      ("wba","12345678"),
      ("wba","12345678"),
    ),3)

    rdd.saveAsTextFile("outpath")
    rdd.saveAsObjectFile("outpath1")
    rdd.saveAsSequenceFile("outpath2")

    //TODO
    //所谓的行动算子，其实就是触发job的执行
    rdd.collect();
    sc.stop()
  }



}
