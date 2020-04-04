package com.xin.hbase;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.io.IOException;

/**
 * Created by xinBa.
 * User: 辛聪明
 * Date: 2020/4/2
 */
public class HbaseUtil {
        HBaseAdmin admin = null;
        Configuration configration = null;
        /**
         * 私有构造方法  配置configuration，获得admin
         */
        private HbaseUtil(){
            configration = new Configuration();
            configration.set("hbase.zookeeper.quorum","hdp-1:2181");
            configration.set("hbase.rootdir","hdfs://hdp-1/hbase");
            try {
                admin = new HBaseAdmin(configration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        静态类对象
        private static HbaseUtil instance = null;
//      同步锁，获取一个不为null的类对象    --> 单例
        public static synchronized HbaseUtil getInstance(){
            if(null == instance){
                instance = new HbaseUtil();
            }
            return instance;
        }
        /**
         * 根据表名获取到 Htable 实例
         */
        public HTable getTable(String tableName){
            HTable table = null;
            try {
                table = new HTable(configration,tableName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return table;
        }
        /**
         * 添加一条记录到 Hbase 表 70 30 128 32 核 200T 8000
         * @param tableName Hbase 表名
         * @param rowkey Hbase 表的 rowkey
         * @param cf Hbase 表的 columnfamily
         * @param column Hbase 表的列
         * @param value 写入 Hbase 表的值
         */
        public void put(String tableName,String rowkey,String cf,String column,String value){
            HTable table = getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowkey));
            put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static void main(String[] args) {
//HTable table = HBaseUtil.getInstance().getTable("category_clickcount");
//System.out.println(table.getName().getNameAsString());
            String tableName = "category_clickcount";
            String rowkey = "20271111_88";
            String cf="info";
            String column ="click_count";
            String value = "2";
            HbaseUtil.getInstance().put(tableName,rowkey,cf,column,value);
        }
}
