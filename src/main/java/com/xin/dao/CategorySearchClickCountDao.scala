package com.xin.dao

import com.xin.hbase.HbaseUtil
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * Created by xinBa.
 * User: 辛聪明
 * Date: 2020/4/4
 * 功能二：功能一+从搜索引擎引流过来的
 * (本类和CategoryClickCountDao除了表名，属性名之外基本一致)
 */
object CategorySearchClickCountDao {

    val tableName = "category_search_clickcount"
    val cf = "info"
    val qualifer = "search_click_count"

    def save(list: ListBuffer[CategarSearchClickCount]): Unit ={
      val table: HTable = HbaseUtil.getInstance().getTable(tableName)
      list.foreach(child=>{
        table.incrementColumnValue(Bytes.toBytes(child.day_search_categary),Bytes.toBytes(cf),
          Bytes.toBytes(qualifer),child.clickCount)
      })
    }

    def count(day_categary:String) : Long={
      val table =HbaseUtil.getInstance().getTable(tableName)
      val get = new Get(Bytes.toBytes(day_categary))
      val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
      if(value == null){
        0L
      }else{
        Bytes.toLong(value)
      }
    }
    def main(args: Array[String]): Unit = {
      val list = new ListBuffer[CategarSearchClickCount]
      list.append(CategarSearchClickCount("20171122_1_8",300))
      list.append(CategarSearchClickCount("20171122_2_9", 600))
      list.append(CategarSearchClickCount("20171122_2_10", 1600))
      save(list)
      print(count("20171122_2_2")+"---")
    }
  case class CategarSearchClickCount(day_search_categary:String,clickCount:Int)
}


