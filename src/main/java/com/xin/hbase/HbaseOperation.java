package com.xin.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by xinBa.
 * User: 辛聪明
 * Date: 2020/4/2
 */
public class HbaseOperation {
        private static Configuration conf;
        private static String ZK_QURUM = "hbase.zookeeper.quorum";
        private static String zk_list = "hdp-1:2181,hdp-2:2181,hdp-3:2181";
        private static String HBASE_POS = "hdp-1";
        private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
        private static final String ZK_PORT_VALUE = "2181";
        private static HConnection hTablePool;

        /*** 静态构造，在调用静态方法前运行，  初始化连接对象  * */
        static {
            //设置连接属性
            conf = HBaseConfiguration.create();  //创建hbase属性
            conf.set(ZK_QURUM, zk_list);          //设置zookeeper集群
            //hdfs下hbase数据位置
            conf.set("hbase.rootdir", "hdfs://" + HBASE_POS + ":9000/hbase");
            conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);   //hbase端口号

            //创建连接池
            try {
                hTablePool = HConnectionManager.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //创建表
        @Test
        public void createTbale() {
            try {
                HBaseAdmin admin = new HBaseAdmin(conf);
                if (admin.tableExists("category_clickcount")) {
                    System.err.println("此表已存在！！！");
                } else {
                    HTableDescriptor descriptor = new HTableDescriptor("category_clickcount");
                    String[] columnFamilys = {"info", "person"};
                    for (String columnFamily : columnFamilys) {
                        descriptor.addFamily(new HColumnDescriptor(columnFamily));
                    }
                    admin.createTable(descriptor);
                    System.err.println("建表成功!");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //添加数据 1
        @Test
        public void insert() {
            try {
                //获取score表
                HTable category = new HTable(conf, "category_clickcount");
//            连接池获取表
//            HTableInterface score = hTablePool.getTable("score");

                //行键，要求byte[]
                byte[] row = "002".getBytes();
                Put put = new Put(row);
                put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("math"));
//                put.add(Bytes.toBytes("info"), Bytes.toBytes("success"), Bytes.toBytes("99"));
//                put.add("category_clickcount".getBytes(),"id".getBytes(),"110".getBytes());
                category.put(put);
                category.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //删除一个表
        @Test
        public void deleteTable() {
            try {
                HBaseAdmin admin = new HBaseAdmin(conf);
                if (admin.tableExists("score")) {
                    admin.disableTable("score");// 禁用表
                    admin.deleteTable("score");// 删除表
                    System.err.println("删除表成功!");
                } else {
                    System.err.println("删除的表不存在！");
                }
                admin.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //查询表中所有行
        @Test
        public void scaner() {
            try {
//            HTable table = new HTable(conf, "score");
                HTableInterface table = hTablePool.getTable("score");
                Scan s = new Scan();
                ResultScanner rs = table.getScanner(s);
                for (Result r : rs) {
                    KeyValue[] kv = r.raw();
                    for (int i = 0; i < kv.length; i++) {
                        System.out.print("键值："+new String(kv[i].getRow()));
                        System.out.print("   列族："+new String(kv[i].getFamily()) + ":");
                        System.out.print(new String(kv[i].getQualifier()) + "");
                        System.out.print("   时间戳:"+kv[i].getTimestamp() + "");
                        System.out.println("  value:"+new String(kv[i].getValue()));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
}
