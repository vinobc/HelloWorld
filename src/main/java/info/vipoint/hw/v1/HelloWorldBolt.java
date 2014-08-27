package info.vipoint.hw.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HelloWorldBolt extends BaseRichBolt {
	private int count;
	public void execute(Tuple tuple) {
		String test = tuple.getStringByField("sentence");
		if("Hello World".equals(test)) {
			count++;
			//System.out.println("Hello World **found** Count is:" + Integer.toString(count));
		}
	}
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
	}
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public void cleanup() {
		System.out.println("**** Word Counts ****");
		System.out.println("Hello World **found** Count is:" + Integer.toString(count));
	}

}
