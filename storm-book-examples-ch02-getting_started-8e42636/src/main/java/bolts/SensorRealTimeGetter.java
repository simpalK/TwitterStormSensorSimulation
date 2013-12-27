package bolts;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

public class SensorRealTimeGetter implements IRichBolt {

	  private static final long serialVersionUID = 1L;
	  private OutputCollector collector;

  	  private String groupIds ="";
  	String str="";
	  public void ack(Object msgId) {
			System.out.println("OK:"+msgId);
		}
		public void close() {}
		public void fail(Object msgId) {
			System.out.println("Not came:"+msgId);
		}
      public  HashMap<Integer,List<String>> groupingSensors = new HashMap<Integer,List<String>>();
      static int[][] topo = new int[][]{
  		{1, 0, 0, 1, 0},
  		{1, 1, 1, 0, 0},
  		{0, 1, 1, 0, 0},
  		{1, 0, 0, 1, 1},
  		{1, 0, 0, 1, 1}
  		};
	  
	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("groupIds","word"));
		  }


	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		String sentence = input.getString(0);
        String[] tokens= sentence.split("\n");
        
        //Computing group Ids for tuples to forward for LISA compute
        for(String word : tokens){
        	String[] senseVal = word.split(",");
            word = word.trim();
            if(!word.isEmpty()){
            	for(int i=0; i<5; i++){
                	if(Integer.parseInt(senseVal[0]) == i){
        			for(int j=0; j<5; j++){
        				groupIds += topo[i][j];
        			} 
                    str += word + ":";
                	for(String strPass : tokens){
                    	String[] sensePass = strPass.split(",");
                    	if(!strPass.trim().isEmpty() && word != strPass){
                    	for(int k=0; k<5; k++){
                        		for(int l=0; l<5; l++){
                        			if(topo[i][l]==1 && Integer.parseInt(sensePass[0]) == l && Integer.parseInt(sensePass[0]) != i && !str.contains(strPass)){
                        				str += strPass + ":";
                        				//System.out.print("looping param "+ "i:=" + i +  "l:=" + l + "k:=" +k+"\n ****");
                        				//System.out.print("String after grouping "+ "group:=" + groupIds +  str +"\n ****");
                        			}
                        			
                        		}
                          }
                    	}
                	}
                	collector.emit(new Values(groupIds,str));
                    System.out.print("groupIds" + groupIds + "word" + str);
                    groupIds = "";
                    str ="";
                	}
                	

                    
                }
                
            }
        }
        collector.ack(input);
        /*for(int i=0; i<5; i++){
        	if(Integer.parseInt(tokens[0]) == i){
			for(int j=0; j<5; j++){
				groupIds += topo[i][j];
			} 
          }
        }*/
		 //for(String word: tokens)
		 //groupIds="";
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}