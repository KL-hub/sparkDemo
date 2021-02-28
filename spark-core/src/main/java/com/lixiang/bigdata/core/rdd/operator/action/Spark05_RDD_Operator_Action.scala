package com.lixiang.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 4),2)
    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("a",3)))

    //TODO 行动算子
    rdd1.saveAsTextFile("outpath")
    rdd1.saveAsObjectFile("outpath1")
    rdd1.saveAsObjectFile("outpath2")
    rdd1.saveAsSequenceFile("outpath3")
    sc.stop()
  }
}
