package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  sample  将数据 抽取部分样本
 */
object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    // sample算子需要传递三个参数
    //1、第一个参数表示，抽取数据后是否将数据返回
    //2、第二个参数表示，数据源中每条数据被抽取的概率
    //3、第三个参数表示，抽取数据随机算法的种子
    println(rdd.sample(false,0.4,1).collect().mkString(","))
    sc.stop()
    }

}
