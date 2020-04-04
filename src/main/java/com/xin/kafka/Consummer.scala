package com.xin.kafka

import com.xin.dao.{CategaryClickCountDAO, CategorySearchClickCountDao}
import com.xin.dao.CategaryClickCountDAO.CategaryClickCount
import com.xin.dao.CategorySearchClickCountDao.CategarSearchClickCount
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by xinBa.
 * User: 辛聪明
 * Date: 2020/4/2
 */
object Consummer {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("shishi")
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hdp-1:2181", //zookeeper
      "flumeTopic", //消费者组groupid
      Map("flumeTopic" -> 3) //map中存放多个topic主题，格式为：<topic,分区数>
    )
    //将消费的数据转成DStream     kafka传过来数据是<k,v> k默认null，v是我们输入的值
    val logs: DStream[String] = kafkaDStream.flatMap((tuple: (String, String)) =>tuple._2.split(","))

    /**
     * 一、清洗数据，过滤出无用数据.将数据封装成DStream[clickLog]
     * 日志格式：143.29.187.156	2020-04-01 17:42:41	"GET www/4 HTTP/1.0"	https://www.sogou.com/web?qu=猎场	404
     */
    val cleanData: DStream[clickLog] = logs.map(line =>{
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      var categaryId = 0
      //把爱奇艺的类目编号拿到了
      if(url.startsWith("www")){
        categaryId = url.split("/")(1).toInt
      }
//      infos(0)-->ip   infos(1)-->date  categaryId-->节目编号
//      infos(4)-->状态码  infos(3):搜索方式
      clickLog(infos(0),DataUtils.parseToMinute(infos(1)),categaryId,infos(4).toInt,infos(3))
    }).filter((clickLog: clickLog) =>clickLog.categaryId != 0)

    /**
     * 二、保存收集数据到 HBase里面
     * 功能需求：每个类别每天的点击量
     */
    cleanData.map(log=>{
      //date:yyyyMMdd
      (log.date.substring(0,8)+"_"+log.categaryId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition((partriosRdds: Iterator[(String, Int)]) =>{
        val list = new ListBuffer[CategaryClickCount]
        partriosRdds.foreach(pair=>{
          list.append(CategaryClickCount(pair._1,pair._2))
        })
        //将结果数据保存到hbase，计数器实现同rowkey的count累加
        CategaryClickCountDAO.save(list)
      })
    })

    /**
     * 三、保存收集数据到 HBase里面
     * 功能需求：从搜索引擎引流过来的每个类别每天的点击量
     */
    cleanData.map(log=>{
      //      https://www.sogou.com/web?qu=猎场
      val refren: String = log.types
      val strings: Array[String] = refren.replaceAll("//","/").split("/")
      var host = ""
      if(strings.length>2){
        host = strings(1)
      }
      (host,log.categaryId,log.date)
    }).filter(_._1 != "").map(x=>{
      (x._3.substring(0,8)+"_"+x._1+"_"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition((partriosRdds: Iterator[(String, Int)]) =>{
        val list = new ListBuffer[CategarSearchClickCount]
        partriosRdds.foreach(pair=>{
          list.append(CategarSearchClickCount(pair._1,pair._2))
        })
        //将结果数据保存到hbase，计数器实现同rowkey的count累加
        CategorySearchClickCountDao.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class clickLog(ip:String,date : String,categaryId:Int,statusid:Int,types:String)
}
