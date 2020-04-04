package com.xin.dao

import com.xin.hbase.HbaseUtil
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 功能开发：今天到现在为止，每个栏目的访问量
  */
object CategaryClickCountDAO {

     val tableName = "category_clickcount"
     val cf = "info"
     val qualifer = "click_count"     //属性

    /**
      * 保存数据
      * @param list
      */
    def save(list:ListBuffer[CategaryClickCount]): Unit ={
      val table: HTable =  HbaseUtil.getInstance().getTable(tableName)
        for(els <- list){
          //计数器    相同rowkey(categaryID)，相同列簇、相同属性的amount(clickCout)会相加
            table.incrementColumnValue(Bytes.toBytes(els.categaryID),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCout);
        }
    }

  /**
   * 获取指定rowkey的指定列簇指定属性的值，null-->0L not null-->转换long类型
   * 主要功能：获取count数量
   * @param day_categary
   */
    def count(day_categary:String) : Long={
        val table: HTable =HbaseUtil.getInstance().getTable(tableName)   //获取表
        val get = new Get(Bytes.toBytes(day_categary))            //获取指定rowkey数据
        val  value: Array[Byte] =  table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
         if(value == null){
           0L
         }else{
             Bytes.toLong(value)
         }
    }

    def main(args: Array[String]): Unit = {
//       val list = new ListBuffer[CategaryClickCount]
//        list.append(CategaryClickCount("20171122_1",1))
//        list.append(CategaryClickCount("20171122_9", 2))
//        list.append(CategaryClickCount("20171122_10", 3))
//        save(list)

        print(count("20200404_4"))
    }

  case class CategaryClickCount(categaryID:String,clickCout:Int)
}
