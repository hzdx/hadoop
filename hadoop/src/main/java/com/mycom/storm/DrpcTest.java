package com.mycom.storm;

import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.security.auth.SimpleTransportPlugin;
import org.apache.storm.utils.DRPCClient;

public class DrpcTest {

	private static String thriftTransport = SimpleTransportPlugin.class.getName();
	public static void main(String[] args) throws Throwable {
		Config conf = new Config();
		conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, thriftTransport);
		conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 2000);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 6000);
		conf.put(Config.DRPC_MAX_BUFFER_SIZE, 104857600); // 100M
		DRPCClient client = new DRPCClient(conf,"localhost", 3772);
		System.out.println(client.execute("words", "cat dog the man"));

	}

}
