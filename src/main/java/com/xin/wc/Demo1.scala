package com.xin.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xinBa.
 * User: 辛聪明
 * Date: 2020/3/30
 */
object Demo1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test1"))

//    val lines: RDD[String] = sc.textFile("hdfs://hdp-1:9000/spark")

    val lines: RDD[String] = sc.textFile(args(0))

    //切分压平 _代表一行内容
    val words: RDD[String] = lines.flatMap(_.split(","))
    //将单词和一组合，形成一个元组
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合  _+_是1+1，2+1，3+1 .....
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //  wordAndOne.reduceByKey((x:Int,y:Int)=>x+y)
    //排序   _._2 根据第二个数据排序 <hello,3>根据3排序，false表示降序，ascending=false
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
