package com.mycom.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) throws IOException {
		MyBiz proxy = (MyBiz) RPC.getProxy(MyBiz.class, MyBiz.versionID,
				new InetSocketAddress(MyServer.SERVER_ADDRESS, MyServer.SERVER_PORT), new Configuration());
		String result = proxy.hello("5");
		System.out.println(result);
		RPC.stopProxy(proxy);
	}
}