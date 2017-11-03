package com.mycom.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase java api 可以远程访问
 *
 */
public class HbaseTest {
	static Configuration conf = null;

	static {
		// 加载hbase配置，需要指定远程zookeeper地址
		// 实际上 是先访问zookeeper，然后zookeeper访问hbase的-ROOT-和.META表，
		// 经过了多次网络请求
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.16.132");
		
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		conf.addResource("hbase-site.xml");

	}

	/***
	 * 创建一张表 并指定列簇
	 */
	public void createTable(String tableName, String... cols) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);// 客户端管理工具类
		if (admin.tableExists(tableName)) {
			System.out.println("此表已经存在.......");
		} else {
			HTableDescriptor table = new HTableDescriptor(tableName);
			for (String c : cols) {
				HColumnDescriptor col = new HColumnDescriptor(c);// 列簇名
				table.addFamily(col);// 添加到此表中
			}

			admin.createTable(table);// 创建一个表
			admin.close();
			System.out.println("创建表成功!");
		}
	}

	/**
	 * 添加数据, 建议使用批量添加
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            行号
	 * @param columnFamily
	 *            列簇
	 * @param column
	 *            列
	 * @param value
	 *            具体的值
	 * 
	 **/
	public static void insertRow(String tableName, String row, String columnFamily, String column, String value)
			throws Exception {
		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(row));
		// 参数出分别：列族、列、值
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

		table.put(put);
		table.close();// 关闭
		System.out.println("插入一条数据成功!");
	}

	/**
	 * 删除一条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            rowkey
	 **/
	public static void deleteByRow(String tableName, String rowkey) throws Exception {
		HTable h = new HTable(conf, tableName);
		Delete d = new Delete(Bytes.toBytes(rowkey));
		h.delete(d);// 删除一条数据
		h.close();
	}

	/**
	 * 删除多条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            rowkey
	 **/
	public static void deleteByRow(String tableName, String rowkey[]) throws Exception {
		HTable h = new HTable(conf, tableName);

		List<Delete> list = new ArrayList<Delete>();
		for (String k : rowkey) {
			Delete d = new Delete(Bytes.toBytes(k));
			list.add(d);
		}
		h.delete(list);// 删除
		h.close();// 释放资源
	}

	/**
	 * 输出一条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowkey
	 *            行号
	 ***/
	public static void getOneDataByRowKey(String tableName, String rowkey) throws Exception {
		HTable h = new HTable(conf, tableName);

		Get g = new Get(Bytes.toBytes(rowkey));
		Result r = h.get(g);
		System.out.println(r);
		for (KeyValue k : r.raw()) {

			System.out.println("行号:  " + Bytes.toStringBinary(k.getRow()));
			System.out.println("时间戳:  " + k.getTimestamp());
			System.out.println("列簇:  " + Bytes.toStringBinary(k.getFamily()));
			System.out.println("列:  " + Bytes.toStringBinary(k.getQualifier()));
			// if(Bytes.toStringBinary(k.getQualifier()).equals("myage")){
			// System.out.println("值: "+Bytes.toInt(k.getValue()));
			// }else{
			String ss = Bytes.toString(k.getValue());
			System.out.println("值:  " + ss);
			// }

		}
		h.close();

	}

	/**
	 * 扫描所有数据或特定数据
	 * 
	 * @param tableName
	 **/
	public static void showAll(String tableName) throws Exception {

		HTable h = new HTable(conf, tableName);

		Scan scan = new Scan();
		// 扫描特定区间
		// Scan scan=new Scan(Bytes.toBytes("开始行号"),Bytes.toBytes("结束行号"));
		ResultScanner scanner = h.getScanner(scan);
		for (Result r : scanner) {
			System.out.println("==================================");
			for (KeyValue k : r.raw()) {

				System.out.println("行号:  " + Bytes.toStringBinary(k.getRow()));
				System.out.println("时间戳:  " + k.getTimestamp());
				System.out.println("列簇:  " + Bytes.toStringBinary(k.getFamily()));
				System.out.println("列:  " + Bytes.toStringBinary(k.getQualifier()));
				// if(Bytes.toStringBinary(k.getQualifier()).equals("myage")){
				// System.out.println("值: "+Bytes.toInt(k.getValue()));
				// }else{
				String ss = Bytes.toString(k.getValue());
				System.out.println("值:  " + ss);
				// }

			}
		}
		h.close();

	}

	/**
	 * 删除表
	 */
	public static boolean deleteTable(String tablename) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				admin.close();
				return false;
			}
		}
		admin.close();
		return true;
	}

	public static void main(String[] args) throws Exception {
		// deleteByRow("test","row1");
		//getOneDataByRowKey("test", "row2");
		// insertRow("test","row2","cf","ddd","abc");
		 showAll("dh");

		// createTable("test2", "testcf");
		// deleteTable("apitable");

	}

}
