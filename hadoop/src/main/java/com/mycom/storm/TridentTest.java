package com.mycom.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TridentTest {

	public static void main(String[] args) throws Throwable {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("the cow jumped over the moon"),
				new Values("the man went to the store and bought some candy"),
				new Values("four score and seven years ago"), new Values("how many apples can you eat"));
		spout.setCycle(true);

		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology.newStream("spout1", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(6);

		Config conf = new Config();
		conf.setDebug(false);
		conf.put("drpc.http.port",3772);

		System.out.println(conf.get("drpc.http.port"));
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology.build());
		Utils.sleep(100000);
		cluster.killTopology("test");
		cluster.shutdown();

	}

}
