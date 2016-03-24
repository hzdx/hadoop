package com.mycom.hadoop.hbase;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * hbase java api?
 *
 */
public class HbaseTest {
	static Configuration conf = null;

	static {
		Configuration conf = new Configuration();
//		FileReader is = new FileReader(new FileInputStream(
//				"E:/github/hadoop/hadoop/src/main/resources/hbase-site.xml"));
		conf.addResource("E:/github/hadoop/hadoop/src/main/resources/hbase-site.xml");
		//加载hbase配置，具体需要哪些配置不太清楚
		conf = HBaseConfiguration.create(conf);
	
	}

	/**
	 */
	public static void createTable(String tablename, String columnFamily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("Table exists!");
			System.exit(0);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
		admin.close();

	}

	/**
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

	/**
	 */
	public static void putCell(HTable table, String rowKey, String columnFamily, String identifier, String data)
			throws Exception {
		Put p1 = new Put(Bytes.toBytes(rowKey));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier), Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put '" + rowKey + "', '" + columnFamily + ":" + identifier + "', '" + data + "'");
	}

	/**
	 */
	public static Result getRow(HTable table, String rowKey) throws Exception {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		System.out.println("Get: " + result);
		return result;
	}

	/**
	 */
	public static void deleteRow(HTable table, String rowKey) throws Exception {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		System.out.println("Delete row: " + rowKey);
	}

	/**
	 */
	public static ResultScanner scanAll(HTable table) throws Exception {
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	/**
	 */
	public static ResultScanner scanRange(HTable table, String startrow, String endrow) throws Exception {
		Scan s = new Scan(Bytes.toBytes(startrow), Bytes.toBytes(endrow));
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	/**
	 */
	public static ResultScanner scanFilter(HTable table, String startrow, Filter filter) throws Exception {
		Scan s = new Scan(Bytes.toBytes(startrow), filter);
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		//HTable table = new HTable(conf, "test");

		// ResultScanner rs = HBaseDAO.scanRange(table, "2013-07-10*",
		// "2013-07-11*");
		// ResultScanner rs = HBaseDAO.scanRange(table, "100001", "100003");
//		ResultScanner rs = HbaseTest.scanAll(table);
//
//		for (Result r : rs) {
//			System.out.println("Scan: " + r);
//		}
//		table.close();

		createTable("apitable", "testcf");
		// HBaseDAO.putRow("apitable", "100001", "testcf", "name", "liyang");
		// HBaseDAO.putRow("apitable", "100003", "testcf", "name", "leon");
		// HBaseDAO.deleteRow("apitable", "100002");
		// HBaseDAO.getRow("apitable", "100003");
		// HBaseDAO.deleteTable("apitable");

	}

}
