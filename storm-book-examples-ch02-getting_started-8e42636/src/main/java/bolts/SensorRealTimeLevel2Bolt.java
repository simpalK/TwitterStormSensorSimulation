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
      static int[][] topo = new int[][]{
  		{1, 0, 0, 1, 0},
  		{1, 1, 1, 0, 0},
  		{0, 1, 1, 0, 0},
  		{1, 0, 0, 1, 1},
  		{1, 0, 0, 1, 1}
  		};
      @Override  
  	public void cleanup() {
  		System.out.println("-- Sensor Level 2 Bolt ["+name+"-"+id+"] --");
  		for(Map.Entry<String, String> entry : counters.entrySet()){
  			System.out.println(entry.getKey()+": "+entry.getValue());
  		}
  	}
      @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	  
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
		String[] sensorValues = dat.split(":");
		String[] vAValues= sensorValues[0].split(",");
		Integer vA = Integer.parseInt(vAValues[1]);
		Double sumOfNeighbors = 0.0;
		

		Integer[] findNeighborsVal = new Integer[1000];
		//Filter Neighbors values at same time stamp
		for(String senseVal: sensorValues){
			String[] vKValues= senseVal.split(",");
			if(vKValues[2] == vAValues[2]){
				findNeighborsVal[counterVK++]  = Integer.parseInt(vKValues[1]);
				System.out.print("Find Neighbors: " +findNeighborsVal[counterVK-1] + "\n");
			}
			System.out.print("Neighbors: " +vKValues[2] +","+vAValues[2] + "\n");

		}
		
		//Compute sum of all neighbors
		for(int i =0;i<findNeighborsVal.length;i++){
			if(findNeighborsVal[i]!=null)
			sumOfNeighbors += findNeighborsVal[i];
		}
		//Compute mean of all neighbors
		Double meanOfNeighbors =sumOfNeighbors/findNeighborsVal.length;
		Double sumSquares=0.0;
		//Compute Standrd Deviation of all neighbors
		for(int i =0;i<findNeighborsVal.length;i++){
			if(findNeighborsVal[i]!=null)
			sumSquares += (meanOfNeighbors - findNeighborsVal[i])*(meanOfNeighbors - findNeighborsVal[i]);
		}	
		Double standardDeviation = Math.sqrt(sumSquares/findNeighborsVal.length);
		
		//Compute LISA Algorithm
		Double lisaEqPart1 = (vA - meanOfNeighbors)/standardDeviation;
		Double lisaPart2 = 0.0;
		if(findNeighborsVal.length>1){
		for(int i = 1;i<findNeighborsVal.length; i++){
			if(findNeighborsVal[i]!=null)
		  lisaPart2 += (1/findNeighborsVal.length) * ((findNeighborsVal[i]-meanOfNeighbors)/standardDeviation);	
		}
		}
		computeLisa = lisaEqPart1 * lisaPart2;
		
		
		System.out.print("Bolt2: " + gid + "Information: " + vAValues[0] + vAValues[1] + vAValues[2] + "Va: "+vA+  "LISA Value:" + computeLisa + "\n");
		collector.ack(input);
		counterVK =0;
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}