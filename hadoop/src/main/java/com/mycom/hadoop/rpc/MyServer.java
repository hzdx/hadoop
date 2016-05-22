package com.mycom.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {
	public static final String SERVER_ADDRESS = "localhost";
	public static final int SERVER_PORT = 12345;

	public static void main(String[] args) throws Exception {
		// 相对于以前的版本有略微的改动
		RPC.Builder ins = new RPC.Builder(new Configuration());
		ins.setInstance(new MyBizImpl());
		ins.setBindAddress(SERVER_ADDRESS);
		ins.setPort(SERVER_PORT);
		ins.setProtocol(MyBiz.class);
		// RPC.setProtocolEngine(new Configuration(), MyRPCProtocal.class,
		// RpcEngine.class);
		Server server = ins.build();// 获得一个server实例
		server.start();
		server.join();
	}
}
