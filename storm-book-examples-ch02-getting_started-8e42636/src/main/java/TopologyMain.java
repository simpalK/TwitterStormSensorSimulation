import spouts.SensorEmitter;
import spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.LISABoltOutputJSon;
import bolts.SensorRealTimeGetter;
import bolts.SensorRealTimeLevel2Bolt;
import bolts.WordCounter;
import bolts.WordNormalizer;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("SensorEmitter",new SensorEmitter(),2);
		builder.setBolt("SensorGetter", new SensorRealTimeGetter(),4)
			.allGrouping("SensorEmitter");
		builder.setBolt("SensorBolt2", new SensorRealTimeLevel2Bolt(),6)
		.fieldsGrouping("SensorGetter", new Fields("groupIds"));
		builder.setBolt("SensorLisaBolt", new LISABoltOutputJSon(),6)
		.shuffleGrouping("SensorBolt2");
		/*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("SensorTtopology", conf, builder.createTopology());
        Thread.sleep(600000);
        //cluster.killTopology("SensorTry5topology");   
        //cluster.shutdown();*/
		        //Configuration
		

		Config conf = new Config();
		conf.setMaxTaskParallelism(3);
        conf.setNumWorkers(3);
        conf.setMaxSpoutPending(5000);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 3);
        conf.setDebug(true);
		conf.put("wordsFile", args[0]);
		                
		                
		//                conf.setMaxSpoutPending(5000);
		//                conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 3);
		//System.setProperty("storm.jar", "/home/simpal/storm-book-examples-ch02-getting_started-8e42636/target/Getting-Started-0.0.1-SNAPSHOT.jar");
		try {
		        StormSubmitter.submitTopology("SensorT113topology", conf,
		                builder.createTopology());
		        Thread.sleep(30000);
		        
		    } catch (AlreadyAliveException e) {
		        // TODO Auto-generated catch block
		        e.printStackTrace();
		    } catch (InvalidTopologyException e) {
		        // TODO Auto-generated catch block
		        e.printStackTrace();
		    }
		        /*Topology run
		                //LocalCluster cluster = new LocalCluster();
		                StormSubmitter.submitTopology("mytopo", conf, builder.createTopology());
		                Thread.sleep(30000);
		                //cluster.shutdown();
				/*Config conf = new Config();
				conf.put("wordsFile", args[0]);
				conf.setDebug(false);
				
		conf.setNumWorkers(3);
		conf.setMaxSpoutPending(5000);

		        //Topology run
				conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
				StormSubmitter.submitTopology("myTopologie", conf, builder.createTopology());
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
				Thread.sleep(1000);
				cluster.shutdown();*/	}
}
