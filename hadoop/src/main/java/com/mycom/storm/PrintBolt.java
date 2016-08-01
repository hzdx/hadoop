package com.mycom.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String rec = tuple.getString(0);
		System.out.println("String recieved: " + rec);//打印Bolt
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//不需要向其他bolt发送数据了， do nothing
	}

}