package com.xin.mysql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xinBa.
 * User: 辛聪明
 * Date: 2020/3/29
 */
object ClickLog {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkJob2")
    val sc = new SparkContext(conf)

//    val text: RDD[String] = sc.textFile("hdfs:hdp-1:9000/data/clickLog/2020/03/27xxxxx_click_log_access.log.12020_03_28_21_58_44")
    val text: RDD[String] = sc.textFile(args(0))
    val ipRdd: RDD[(String, Float)] = text.map(x => {
      val strings: Array[String] = x.split(" ")
      val ip = strings(0);
      //      val date = AnalysisNginxTool.nginxDateStmpToDate(strings(3));
      //      val url = strings(6);
      val upFlow = strings(9).toFloat;
      (ip, upFlow)
    }).reduceByKey((_ + _))

    ipRdd.foreachPartition(insertData)
    sc.stop()
  }

  def insertData(iterator: Iterator[(String, Float)]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hdp-1:3306/sqoop", "root", "123456")
    iterator.foreach(data => {
      data
      val ps = conn.prepareStatement("insert into upflow(ip,sum) values (?,?)")
      ps.setString(1,data._1)
      ps.setFloat(2,data._2)
      ps.executeUpdate()
    })
  }
}
