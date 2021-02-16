package com.lixiang.bigdata.core.test

class Task extends Serializable {
  val datas =List(1,2,3,4)

  val logic:(Int)=>Int=_*2

  def comppute():List[Int]={
    datas.map(logic)
  }
}
