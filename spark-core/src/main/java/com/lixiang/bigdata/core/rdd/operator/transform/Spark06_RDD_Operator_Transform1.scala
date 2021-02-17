package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  group by 将数据 进行分组
 */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd: RDD[String] = sc.makeRDD(List("hello","Scala", "Hello" ,"Spark"), 2)
    //分组  与分区  没有必然的额联系
    // groupp by 将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupRdd.collect().foreach(println)
    sc.stop()
    }

}
