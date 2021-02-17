package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  group by 将数据 进行分组
 */
object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4),2)
    // groupp by 将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    def groupFunction(num:Int):Int={
       num%2
    }

    val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    groupRdd.collect().foreach(println)
    sc.stop()
    }

}
