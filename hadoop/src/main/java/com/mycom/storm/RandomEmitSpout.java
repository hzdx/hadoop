package com.mycom.storm;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


/**
 * 该Spout代码里面最核心的部分有两个：<br>
 * 
 * a. 用collector.emit()方法发射tuple。<br>
 * 我们不用自己实现tuple，我们只需要定义tuple的value，Storm会帮我们生成tuple。<br>
 * Values对象接受变长参数。Tuple中以List存放Values，<br>
 * List的Index按照new Values(obj1, obj2,...)的参数的index,<br>
 * 例如我们emit(new Values("v1", "v2")), 那么Tuple的属性即为：{ [ "v1" ], [ "V2" ] }<br>
 * 
 * b. declarer.declare方法用来给我们发射的value在整个Stream中定义一个别名。可以理解为key。
 * 该值必须在整个topology定义中唯一。
 *
 */
@SuppressWarnings("serial")
public class RandomEmitSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private Random rand;

	private static final String[] sentences = new String[] { "edi:I'm happy", "marry:I'm angry", "john:I'm sad",
			"ted:I'm excited", "laden:I'm dangerous" };

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.rand = new Random();
	}

	@Override
	public void nextTuple() {
		String toSay = sentences[rand.nextInt(sentences.length)];
		this.collector.emit(new Values(toSay));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}