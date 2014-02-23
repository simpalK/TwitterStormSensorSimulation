package bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

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
	  String[] sensorsIds = {"N-H563T",	"N-QWNZH",	"N-LETTK",	"N-SCK04",	"N-8HOVD",	"N-2GWON",	"N-UFCUA",	"N-6PFYW",	"N-TZD20",	"N-WRYAZ",	"N-3IK0Y",	"N-JQ338",	"N-Y47X6",	"N-2Z2WK",	"N-GRDHN",	"N-L04BJ"};

     /* static int[][] topo = new int[][]{
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
      
      int[][] topoFromFile= new int[16][16];


	  
	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("groupIds","word"));
		  }


	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Scanner inputStream = null;
		try
		{
		  inputStream = new Scanner(new File("/home/simpal/stormSensorReco/SensorSimulation/SensorSimulations/StormSensorApp/topologyInformation.txt"));//The txt file is being read correctly.
		}
		catch(FileNotFoundException e)
		{
		  System.exit(0);
		}
        
		for (int row = 0; row < 16; row++) {
		    String line = inputStream.nextLine();
		    String[] lineValues = line.split(",");
		  for (int column = 0; column < 16; column++) {
		    topoFromFile[row][column] = Integer.parseInt(lineValues[column]);
		  }
		}
		inputStream.close();
		
		String sentence = input.getString(0);
        String[] tokens= sentence.split("\n");
        
        //Computing group Ids for tuples to forward for LISA compute
        for(String word : tokens){
        	String[] senseVal = word.split(",");
			//System.out.print("sensorVal at second BOLT =" + senseVal[0] +  "\n ****");

            //word = word.trim();
            if(!word.isEmpty()){
            	for(int i=0; i<16; i++){
                	if(senseVal[0].contains(sensorsIds[i])){
        			for(int j=0; j<16; j++){
        				groupIds += topoFromFile[i][j];
        				//System.out.print("generate group ids "+ "group:=" + groupIds +  str +"\n ****");

        			} 
                    str += word + ":";
                	for(String strPass : tokens){
                    	String[] sensePass = strPass.split(",");
                    	if(!strPass.trim().isEmpty() && word != strPass){
                    	for(int k=0; k<16; k++){
                        		for(int l=0; l<16; l++){
                        			if(topoFromFile[i][l]==1 && sensePass[0].contains(sensorsIds[l]) && !sensePass[0].contains(sensorsIds[i]) && !str.contains(strPass)){
                        				str += strPass + ":";
                        				//System.out.print("looping param "+ "i:=" + i +  "l:=" + l + "k:=" +k+"\n ****");
                        				System.out.print("String after grouping "+ "group:=" + groupIds +  str +"\n ****");
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