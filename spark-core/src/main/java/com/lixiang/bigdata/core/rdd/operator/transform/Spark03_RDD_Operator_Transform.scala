package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitions 可以以分区为单位进行数据转换操作
 *                但是会将整个分区的数据加兹安到内存中进行引用
 *                如果处理完的数据不会被释放掉，存在对象的引用
 *                如内存较小，数据量较大的场合下，容易出现内存溢出
 */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index,iter) => {
      if(index==1){
        iter
      }else{
        Nil.iterator
      }

    })
    mapRdd.collect().foreach(println)
    sc.stop()
    }

}
