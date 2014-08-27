package info.vipoint.hw.v1;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HelloWorldSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private int random;
	private static final int RANDOM_MAX = 10;
	
	public HelloWorldSpout() {
		final Random r = new Random();
		random = r.nextInt(RANDOM_MAX);
	}
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	public void nextTuple() {
		Utils.sleep(100);
		final Random r = new Random();
		int rInstance = r.nextInt(RANDOM_MAX);
		if(rInstance == random) {
			collector.emit(new Values("Hello World"));
		} else {
			collector.emit(new Values("something else .."));
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
