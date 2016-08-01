package com.mycom.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * *********Storm主要组件*********<br>
 * <b>Topology</b>：storm中运行的一个实时应用程序，因为各个组件间的消息流动形成逻辑上的一个拓扑结构。<br>
 * 
 * <b>Spout</b>：在一个topology中产生源数据流的组件。通常情况下spout会从外部数据源中读取数据，然后转换为topology内部的源数据。<br>
 * Spout是一个主动的角色，其接口中有个nextTuple()函数，storm框架会不停地调用此函数，用户只要在其中生成源数据即可。<br>
 * 
 * <b>Bolt</b>：在一个topology中接受数据然后执行处理的组件。Bolt可以执行过滤、函数操作、合并、写数据库等任何操作。<br>
 * Bolt是一个被动的角色，其接口中有个execute(Tuple input)函数,在接受到消息后会调用此函数，用户可以在其中执行自己想要的操作。<br>
 * 
 * <b>Tuple</b>：一次消息传递的基本单元。本来应该是一个key-value的map，但是由于各个组件间传递的tuple的字段名称已经事先定义好，<br>
 * 所以tuple中只要按序填入各个value就行了，所以就是一个value list.<br>
 * 
 * <b>Stream</b>：源源不断传递的tuple就组成了stream。<br>
 * stream grouping：即消息的partition方法。<br>
 * Storm中提供若干种实用的grouping方式，包括shuffle, fields hash, all, global, none,
 * direct和localOrShuffle等
 *
 */
public class ExclaimBasicTopo {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomEmitSpout());
		// 定义一个spout，id为"spout"
		builder.setBolt("exclaim", new ExclaimBasicBolt()).shuffleGrouping("spout");
		// 定义了一个id为"exclaim"的bolt，并且按照随机分组获得"spout"发射的tuple
		builder.setBolt("print", new PrintBolt(), 2).shuffleGrouping("exclaim");
		// 定义了一个id为"print"的bolt，并且按照随机分组获得"exclaim”发射出来的tuple,指定了并行度

		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			// 提交到nimbus(主节点)机器上运行,除非手动关闭不然永远不会停止
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			// 本地模式用于开发、测试，模拟一个完整的集群模式
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
			// 一段时间后停掉应用，适用于spout会全部发送完数据的情况
		}
	}

}