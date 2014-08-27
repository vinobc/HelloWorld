package info.vipoint.hw.v1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class HelloWorldTopology {

	private static final String HELLOWORLDSPOUT= "hw-spout";
	private static final String HELLOWORLDSBOLT = "hw-bolt";
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(HELLOWORLDSPOUT, new HelloWorldSpout(),10);
		builder.setBolt(HELLOWORLDSBOLT, new HelloWorldBolt(),3).shuffleGrouping(HELLOWORLDSPOUT);
		
		//for topology's rt behavior
		Config config = new Config();
		config.setDebug(true);
		
		if(args!=null && args.length > 0) {
			config.setNumWorkers(3);
			
			//submit topology to a remote cluster
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		} else {
			//storm's local mode
			LocalCluster cluster = new LocalCluster();
			
			//submit topology to the local cluster
			cluster.submitTopology("hwt", config, builder.createTopology());
			
			backtype.storm.utils.Utils.sleep(10000);
			cluster.killTopology("hwt");
			cluster.shutdown();
		}
	}

}
