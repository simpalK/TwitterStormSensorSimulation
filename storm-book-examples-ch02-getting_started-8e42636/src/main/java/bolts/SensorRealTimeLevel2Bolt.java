package bolts;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.lang.Math;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SensorRealTimeLevel2Bolt implements IRichBolt {

	  private static final long serialVersionUID = 1L;
	  private OutputCollector collector;
	  Integer id;
	  String name;
	  Map<String, String> counters;
	  int counterVK =0;
	  Double computeLisa=0.0;
      public  HashMap<Integer,List<String>> groupingSensors = new HashMap<Integer,List<String>>();
	  String[] sensorsIds = {"N-H563T",	"N-QWNZH",	"N-LETTK",	"N-SCK04",	"N-8HOVD",	"N-2GWON",	"N-UFCUA",	"N-6PFYW",	"N-TZD20",	"N-WRYAZ",	"N-3IK0Y",	"N-JQ338",	"N-Y47X6",	"N-2Z2WK",	"N-GRDHN",	"N-L04BJ"};

      /*static int[][] topo = new int[][]{
    	  {1,	1,	0,	0,	1,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0},
          {1,	1,	1,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0},
          {0,	1,	1,	0,	0,	0,	1,	0,	0,	0,	0,	0,	0,	0,	0,	0},
          {0,	0,	0,	1,	0,	0,	0,	1,	0,	0,	0,	0,	0,	0,	0,	0},
          {1,	0,	0,	0,	1,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0},
          {0,	0,	0,	0,	0,	1,	1,	0,	0,	1,	0,	0,	0,	0,	0,	0},
          {0,	0,	1,	0,	0,	1,	1,	1,	0,	0,	0,	0,	0,	0,	0,	0},
          {0,	0,	0,	1,	0,	0,	0,	1,	0,	0,	0,	0,	0,	0,	0,	0},
          {0,	0,	0,	0,	0,	0,	0,	0,	1,	1,	0,	0,	0,	0,	0,	0},
          {0,	0,	0,	0,	0,	1,	0,	0,	1,	1,	1,	0,	0,	1,	0,	0},
          {0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	1,	1,	0,	0,	0,	0},
          {0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	1,	0,	0,	0,	1},
          {0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	1,	0,	0},
          {0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	0,	0,	1,	1,	0,	0},
          {0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	1},
          {0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	0,	0,	1,	1}
  		};*/
      
     
      @Override  
  	public void cleanup() {
  		/*System.out.println("-- Sensor Level 2 Bolt ["+name+"-"+id+"] --");
  		for(Map.Entry<String, String> entry : counters.entrySet()){
  			System.out.println(entry.getKey()+": "+entry.getValue());
  		}*/
  	}
      @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("gid","senseVal","lisaVal"));

      }
	  
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.counters = new HashMap<String, String>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		
	}
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String gid = input.getString(0);
		String dat = input.getString(1);
		Double mean = input.getDouble(2);
		Double variance = input.getDouble(3);
		String lastTimeStamp = input.getString(4);
		
		String[] sensorValues = dat.split(":");
		//String[] vAValues= sensorValues[0].split(",");
		String[] vAValues= sensorValues[0].split(",");

		Double vA = Double.parseDouble(vAValues[1]);
		Double sumOfNeighbors = 0.0;
		

		Double[] findNeighborsVal = new Double[1000];
		//Filter Neighbors values at same time stamp
		for(String senseVal: sensorValues){
			String[] vKValues= senseVal.split(",");
			if(vKValues[2].contentEquals(lastTimeStamp) && !vKValues[0].contentEquals(vAValues[0])){
				findNeighborsVal[counterVK++]  = Double.parseDouble(vKValues[1]);
				System.out.print("Find Neighbors: " +findNeighborsVal[counterVK-1] + "\n");
			}
		}
		
		//Compute sum of all neighbors
		for(int i =0;i<counterVK;i++){
			if(findNeighborsVal[i]!=null)
			sumOfNeighbors += findNeighborsVal[i];
		}
		//Compute mean of all neighbors
		Double meanOfNeighbors = (vA + sumOfNeighbors)/(counterVK+1);
		Double sumSquares=0.0;
		//Compute Standrd Deviation of all neighbors
		for(int i =0;i<counterVK;i++){
			if(findNeighborsVal[i]!=null)
			sumSquares += (meanOfNeighbors - findNeighborsVal[i])*(meanOfNeighbors - findNeighborsVal[i]);
		}	
		System.out.print("Mean of neighbors: " + mean + "\n");
		System.out.print("Number of Neighbors: " + counterVK+ "\n");

		//System.out.print("Sum of Squares: " + sumSquares+ "\n");
		
		Double standardDeviation = Math.sqrt(sumSquares/counterVK);
		System.out.print("Variance: " + variance + "\n");

		
		//Compute LISA Algorithm
		Double lisaEqPart1 = (vA - mean)/variance;
		System.out.print("Lisa Part1: " + lisaEqPart1+ "\n");

		Double lisaPart2 = 0.0;

		for(int i = 0;i<counterVK; i++){
			if(findNeighborsVal[i]!=null)
		  lisaPart2 += ((1/counterVK) * (findNeighborsVal[i]-mean));	
		}
		System.out.print("Lisa Part2: " + lisaPart2+ "\n");

		computeLisa = lisaEqPart1 * lisaPart2;
		
		
		System.out.print("Bolt2: " + gid + "Information: " + vAValues[0] + vAValues[1] + vAValues[2] + "Va: "+vA+  "LISA Value:" + computeLisa + "\n");
		collector.emit(new Values(gid,sensorValues[0],computeLisa));
		collector.ack(input);
		counterVK =0;
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}