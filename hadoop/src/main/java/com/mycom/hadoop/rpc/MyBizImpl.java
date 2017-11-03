package com.mycom.hadoop.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class MyBizImpl implements MyBiz {
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return versionID;
	}

	@Override
	public String hello(String name) {
		System.out.println("invoked");
		return "hello " + name;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
