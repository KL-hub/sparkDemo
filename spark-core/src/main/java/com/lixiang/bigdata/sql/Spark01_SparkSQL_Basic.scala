package com.lixiang.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //创建SparkSql的运行环境
    val sparkSQl: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQl")
    val session: SparkSession = SparkSession.builder().config(sparkSQl).getOrCreate()
    //TODO 执行业务
    //DataFrame
    val df: DataFrame = session.read.json("datas/user.json")
    //
//    df.show
//    df.createTempView("user")
//    session.sql(" select * from user").show()
//    session.sql(" select avg(age) from user").show()

    //DSL
    //在使用DataFrame时，如果涉及到转换操作，需要引入 转换规则
//    import session.implicits._
//    df.select("age","name").show()
//    df.select($"age"+1).show()
//    df.select('age+1).show()

    //TODO  DataSet
    //DataFrame 其实就是特定泛型的DataSet
    val seq = Seq(1, 2, 3, 4)
    //seq.toDS()
    //关闭
    session.close()
  }

}
