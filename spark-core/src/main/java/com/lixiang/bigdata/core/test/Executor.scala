package com.lixiang.bigdata.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器
    val server: ServerSocket = new ServerSocket(9999)
    println("--------------------服务器启动-----------------------")
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val objIn=new ObjectInputStream(in)
    val task: Task = objIn.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.comppute()
    //val i: Int = in.read()
    println("计算结果计算的值"+ints)

    in.close()
    client.close()
    server.close()
  }

}
