package com.lixiang.bigdata.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark01_RDD_Load {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val i: Int = value.reduce(_ + _)
    println(i)
    //TODO
    //所谓的行动算子，其实就是触发job的执行
    val name: LongAccumulator = sc.longAccumulator("name")
    value.foreach(num=>{
      name.add(num)
    })
    println(name.value)
    sc.stop()
  }



}
