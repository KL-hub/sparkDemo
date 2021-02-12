package com.lixiang.bigdata.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    // appliation 调用  spark框架？？
    //TODO 简历与spark的链接
    val sparkConf =new SparkConf().setMaster("local").setAppName("WordCount")
    val sc=new SparkContext(sparkConf)
    //TODO 执行业务操作
    //1、读取文件，获取一行一行的数据
    val lines:RDD[String]=sc.textFile("datas/1.txt")
    //2、将一行数据进行拆分，形成一个一个的单词
    val words:RDD[String]=lines.flatMap(_.split(" "))

    val wordToOne=words.map(word=>(word,1))
    //3、将数据根据单词进行分组，便于统计
    val value = wordToOne.groupBy(t => t._1)
    //4、对分组后的数据进行转换
    var wordToCount=value.map{
      case (word,list)=>{
        list.reduce((t1,t2)=>{
          (t1._1,t1._2+t2._2)
        })
      }
    }
    //5、将转换结果采集到控制台打印出来
    val array:Array[(String,Int)]= wordToCount.collect()
    array.foreach(println)
    //TODO 关闭链接
    sc.stop()
  }
}
