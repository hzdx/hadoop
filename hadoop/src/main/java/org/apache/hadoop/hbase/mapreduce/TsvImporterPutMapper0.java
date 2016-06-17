package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.sql.catalyst.expressions.Encode;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TsvImporterPutMapper0 extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	public static final String CF_DEFAULT = "cf";
	public static final String SEP_DEFAULT = "|";

	private String columnFamily = CF_DEFAULT;
	private String charset;
	private String keyIndexs = "0";
	private String keyStrategies = "0";
	private String separator = SEP_DEFAULT;
	private String rowKeySeparator = SEP_DEFAULT;
	private boolean skipBadLines;
	private Counter badLineCount;

	private String keyEncrypts = "0";
	private String valueEncrypts = "0";

	public boolean getSkipBadLines() {
		return this.skipBadLines;
	}

	public Counter getBadLineCount() {
		return this.badLineCount;
	}

	public void incrementBadLineCount(int count) {
		this.badLineCount.increment(count);
	}

	protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) {
		Configuration conf = context.getConfiguration();

		this.separator = conf.get("importtsv.separator");
		if (this.separator == null)
			this.separator = "\t";
		else {
			this.separator = new String(Base64.decode(this.separator));
		}

		this.columnFamily = conf.get("importtsv.columnFamily", CF_DEFAULT);

		this.charset = conf.get("importtsv.charset", "gbk");

		// 3,2,6,5
		this.keyIndexs = conf.get("importtsv.rowkey.indexs", "0,ts");

		// r,o,ss|yyyy-MM-dd HH:mm:ss|yyyyMMddHHmmss,
		this.keyStrategies = conf.get("importtsv.rowkey.strategies", "r,");

		this.rowKeySeparator = conf.get("importtsv.rowkey.separator", SEP_DEFAULT);

		this.skipBadLines = context.getConfiguration().getBoolean("importtsv.skip.bad.lines", true);
		this.badLineCount = context.getCounter("ImportTsv", "Bad Lines");

		// keynum,chars
		this.keyEncrypts = conf.get("importtsv.rowkey.encrypts", "keynum,chars");

		// chars
		this.valueEncrypts = conf.get("importtsv.value.encrypts", "chars");

		// 初始化
		columnK = keyIndexs.split(",");

	}

	private String[] columnK;
	

	@SuppressWarnings("deprecation")
	public void map(LongWritable offset, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException {
		try {
			String val = value + "";
			String row = buildRowkey(val);
			if (row == null)
				throw new IllegalArgumentException("rowkey不能为空.");

			String nval = buildVal(val);

			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(row));

			Put p = new Put(rowKey.copyBytes());
			p.add(Bytes.toBytes(columnFamily), null, nval.getBytes());

			context.write(rowKey, p);

		} catch (IllegalArgumentException e) {
			if (this.skipBadLines) {
				System.err.println("Bad line at offset: " + offset.get() + ":\n" + e.getMessage());
				incrementBadLineCount(1);
				return;
			}
			throw new IOException(e);
		} catch (NoSuchAlgorithmException e) {
			if (this.skipBadLines) {
				System.err.println("Bad line at offset: " + offset.get() + ":\n" + e.getMessage());
				incrementBadLineCount(1);
				return;
			}
			throw new IOException(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
	}

	private String buildVal(String val) {
		if (valueEncrypts == null || valueEncrypts.trim().length() == 0)
			return val;
		try {
			if (valueEncrypts.equalsIgnoreCase("chars"))
				return encryptChars(val, charset);

			return val;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("不支持字符集[" + e.getMessage() + "]." + e);
		}
	}

	private String buildRowkey(String val) throws NoSuchAlgorithmException {

		String[] values = val.split(separator);
		if (values == null || values.length == 0)
			return null;

		int clen = columnK.length;

		StringBuilder row = new StringBuilder(200);
		for (int i = 0; i < clen; i++) {
			String part = columnK[i];
			if (part.equalsIgnoreCase("ts")) {
				part = System.currentTimeMillis() + "";
			} else {

				Integer index = Integer.parseInt(part);
				if (index > values.length)
					throw new IllegalArgumentException(
							"指定列数[" + keyIndexs + "]大于值[" + val + "]列数,在分隔符[" + separator + "]");
				part = values[index];
			}
			part = change(part, i);
			row.append(part).append(rowKeySeparator);
		}
		row.append(md5Digest(val));
		return row.toString();

	}

	private String md5Digest(String str) throws NoSuchAlgorithmException {

		String temp;
		MessageDigest alg = MessageDigest.getInstance("MD5");
		alg.update(str.getBytes());
		byte[] digest = alg.digest();
		temp = byte2hex(digest);
		return temp;

	}

	private String byte2hex(byte[] bytes) {

		String hs = "";
		String stmp = "";
		for (int i = 0; i < bytes.length; i++) {
			stmp = (java.lang.Integer.toHexString(bytes[i] & 0XFF));
			if (stmp.length() == 1)
				hs = hs + "0" + stmp;
			else
				hs = hs + stmp;
		}
		return hs.toUpperCase();

	}

	private String change(String part, int i) {

		if (keyStrategies == null)
			return part;

		String[] ks = keyStrategies.split(",");
		if (ks.length <= i)
			return part;

		String strategy = ks[i];
		if (strategy.trim().length() == 0) {
			//
		} else if (strategy.equalsIgnoreCase("r")) {
			return new StringBuilder(part).reverse().toString();
		} else if (strategy.equalsIgnoreCase("o")) {
			//
		} else if (strategy.startsWith("ls") || part.startsWith("sl") || part.startsWith("ss")) {
			return buildDateChange(strategy, part);
		} else {
			//
		}

		part = encryptKeyNums(part);
		return part;
	}

	private String buildDateChange(String strategy, String part) {

		String[] ss = strategy.split("\\|");
		if (ss.length < 2)
			return null;

		String type = ss[0];
		try {
			if (type.equalsIgnoreCase("ls")) {//
				String format = ss[1];

				SimpleDateFormat fromSdf = new SimpleDateFormat(format);
				Date date = fromSdf.parse(part);
				return date.getTime() + "";

			} else if (type.equalsIgnoreCase("sl")) {// 返回秒数
				String format = ss[1];

				SimpleDateFormat sdf = new SimpleDateFormat(format);
				Long time = Long.parseLong(part);
				Date date = new Date(time);
				return sdf.format(date);

			} else if (type.equalsIgnoreCase("ss")) {// 返回时间字符串
				String from = ss[1];
				String to = ss[2];

				SimpleDateFormat fromSdf = new SimpleDateFormat(from);
				SimpleDateFormat toSdf = new SimpleDateFormat(to);

				Date date = fromSdf.parse(part);
				return toSdf.format(date);
			}
		} catch (NumberFormatException e) {
		} catch (ParseException e) {
		}
		return part;
	}

	private char[] KEYNUM_ENCRYPT_MAP = "8374012596".toCharArray();
	private char[] KEYNUM_DECRYPT_MAP = "4561379208".toCharArray();

	public String encryptChars(String text, String charset) throws UnsupportedEncodingException {
		byte[] bytes = null;
		if (charset == null || charset.trim().length() == 0)
			bytes = text.getBytes();
		else
			bytes = text.getBytes(charset);
		byte[] nbytes = encryptBytes(bytes);
		return new String(nbytes, charset);
	}

	public String decryptChars(String text, String charset) throws UnsupportedEncodingException {
		byte[] bytes = null;
		if (charset == null || charset.trim().length() == 0)
			bytes = text.getBytes();
		else
			bytes = text.getBytes(charset);
		byte[] nbytes = decryptBytes(bytes);
		return new String(nbytes, charset);
	}

	public String encryptKeyNums(String text) {

		if (text == null)
			return null;

		if (text.trim().length() == 0)
			return "";

		char[] cs = text.toCharArray();
		int len = cs.length;
		char[] ncs = new char[len];
		for (int i = 0; i < len; i++) {
			try {
				int t = Integer.parseInt("" + cs[i]);
				if (t < 0 || t >= 10)
					ncs[i] = 0;
				else
					ncs[i] = KEYNUM_ENCRYPT_MAP[t];
			} catch (NumberFormatException e) {
				ncs[i] = 0;
			}
		}
		return new String(ncs);
	}

	public String decryptKeyNums(String text) {
		if (text == null)
			return null;

		if (text.trim().length() == 0)
			return "";

		char[] cs = text.toCharArray();
		int len = cs.length;
		char[] ncs = new char[len];
		for (int i = 0; i < len; i++) {
			try {
				int t = Integer.parseInt("" + cs[i]);
				if (t < 0 || t >= 10)
					ncs[i] = 0;
				else
					ncs[i] = KEYNUM_DECRYPT_MAP[t];
			} catch (NumberFormatException e) {
				ncs[i] = 0;
			}
		}
		return new String(ncs);
	}

	public byte[] encryptBytes(byte[] bytes) {
		if (bytes == null)
			return null;
		int len = bytes.length;
		byte[] bs = new byte[len];
		for (int i = 0; i < len; i++) {
			bs[i] = (byte) (bytes[i] + 3);
		}
		return bs;
	}

	public byte[] decryptBytes(byte[] bytes) {
		if (bytes == null)
			return null;
		int len = bytes.length;
		byte[] bs = new byte[len];
		for (int i = 0; i < len; i++) {
			bs[i] = (byte) (bytes[i] - 3);
		}
		return bs;
	}

}
