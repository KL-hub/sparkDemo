package com.lixiang.bigdata.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark01_RDD_Load {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val rdd: RDD[String] = sc.textFile("outpath")
    println(rdd.collect().mkString(","))

    val rdd1= sc.objectFile[(String,Int)]("outpath1")
    println(rdd1.collect().mkString(","))


    val rdd2= sc.sequenceFile[String,Int]("outpath1")
    println(rdd2.collect().mkString(","))
    //TODO
    //所谓的行动算子，其实就是触发job的执行

    sc.stop()
  }



}
