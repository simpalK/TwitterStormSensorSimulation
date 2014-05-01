package bolts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import sun.org.mozilla.javascript.tools.shell.Global;
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
	  String fileName;
	  String[] sensorsIds;
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
	  //String[] sensorsIds = new String[GlobalVar.numberOfNodes];
	  //String[] sensorsIds = {"N-H563T",	"N-QWNZH",	"N-LETTK",	"N-SCK04",	"N-8HOVD",	"N-2GWON",	"N-UFCUA",	"N-6PFYW",	"N-TZD20",	"N-WRYAZ",	"N-3IK0Y",	"N-JQ338",	"N-Y47X6",	"N-2Z2WK",	"N-GRDHN",	"N-L04BJ"};
      
	  
	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("groupIds","word","mean","variance","timeStampMean"));
		  }


	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Scanner inputStream = null;
		  int counterVK =0;
		  int counterSensorVal =0;

	      int[][] topoFromFile= new int[GlobalVar.numberOfNodes][GlobalVar.numberOfNodes];

		try
		{
		  inputStream = new Scanner(new File(this.fileName + "/topologyInformation.txt"));//The txt file is being read correctly.
		}
		catch(FileNotFoundException e)
		{
		  System.exit(0);
		}
        
		try {
            FileReader fr = new FileReader(this.fileName +"/nodes_test16.csv");
            sensorsIds = parseCsv(fr, ",", true);
		} catch (IOException e) {
	           e.printStackTrace();
	       }
		for (int row = 0; row < GlobalVar.numberOfNodes; row++) {
		    String line = inputStream.nextLine();
		    String[] lineValues = line.split(",");
		  for (int column = 0; column < GlobalVar.numberOfNodes; column++) {
		    topoFromFile[row][column] = Integer.parseInt(lineValues[column]);
		  }
		}
		inputStream.close();
		//For testing purpose choose neighbors randomly
		/*for(int l=0;l<16;l++)
		{
			for(int m =0; m<16; m++){
				topoFromFile[l][m] = 1;

			
			if(l != m){
				topoFromFile[l][m] = 0;
				}
			else 
				topoFromFile[l][m] = 1;
			}
		}
		for(int l=0;l<16;l++)
		{
			int cnt=12;
		Random randomParameterVal = new Random();
		int high = 15;
		int low = 0;
		while(cnt>0){
		int changeneighbor=randomParameterVal.nextInt(high-low)+low;
		if(changeneighbor != l && topoFromFile[l][changeneighbor]!=1){
			topoFromFile[l][changeneighbor] = 1;
			cnt--;
			}
		}
		}*/
		
		String sentence = input.getString(0);
        String[] tokens= sentence.split("\n");
		for(String senseVal: tokens){
			counterSensorVal++;
		}
		Double[] findTimeStampVal = new Double[100000];
		String[] lastTimeStamp = tokens[counterSensorVal-1].split(",");
		Double sumOfAllSensors = 0.0;
		Double varianceOfAllSensors = 0.0;
		Double varianceSumOfAllSensors = 0.0;
		//Filter Neighbors values at same time stamp
		for(String senseVal: tokens){
			String[] vKValues= senseVal.split(",");
			if(vKValues[2].contentEquals(lastTimeStamp[2])){
				findTimeStampVal[counterVK++]  = Double.parseDouble(vKValues[1]);
				sumOfAllSensors += Double.parseDouble(vKValues[1]);
				//System.out.print("Find value at last time stamp: " +findTimeStampVal[counterVK-1] + "\n");
			}
		}
		Double meanOfAllSensors = sumOfAllSensors/(counterVK-1);

		for(int i=0; i< counterVK; i++){
			varianceSumOfAllSensors += (findTimeStampVal[i] - meanOfAllSensors)*(findTimeStampVal[i] - meanOfAllSensors);
		}
		varianceOfAllSensors = varianceSumOfAllSensors/(counterVK-1);
        //Computing group Ids for tuples to forward for LISA compute
        for(String word : tokens){
        	String[] senseVal = word.split(",");
			//System.out.print("sensorVal at second BOLT =" + senseVal[0] +  "\n ****");

            //word = word.trim();
            if(!word.isEmpty()){
            	for(int i=0; i<GlobalVar.numberOfNodes; i++){
                	if(senseVal[0].contains(sensorsIds[i])){
        			for(int j=0; j<GlobalVar.numberOfNodes; j++){
        				groupIds += topoFromFile[i][j];
        				//System.out.print("generate group ids "+ "group:=" + groupIds +  str +"\n ****");

        			} 
                    str += word + ":";
                	for(String strPass : tokens){
                    	String[] sensePass = strPass.split(",");
                    	if(!strPass.trim().isEmpty() && word != strPass){
                    	for(int k=0; k<GlobalVar.numberOfNodes; k++){
                        		for(int l=0; l<GlobalVar.numberOfNodes; l++){
                        			if(topoFromFile[i][l]==1 && sensePass[0].contains(sensorsIds[l]) && !sensePass[0].contains(sensorsIds[i]) && !str.contains(strPass)){
                        				str += strPass + ":";
                        				//System.out.print("looping param "+ "i:=" + i +  "l:=" + l + "k:=" +k+"\n ****");
                        				//System.out.print("String after grouping "+ "group:=" + groupIds +  str +"\n ****");
                        			}
                        			
                        		}
                          }
                    	}
                	}
                	//if(senseVal[0].contentEquals("N-H82EQ"))
                	collector.emit(new Values(groupIds,str,meanOfAllSensors,varianceOfAllSensors,lastTimeStamp[2]));
                    //System.out.print("groupIds" + groupIds + "word" + str + "mean" +meanOfAllSensors+  "variance"+varianceOfAllSensors +"\n");
                    groupIds = "";
                    str ="";
                	//}
                	

                    
                }
                
            }
        }
        collector.ack(input);
        }
		
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
	
	public String[] parseCsv(Reader reader, String separator, boolean hasHeader) throws IOException {
        //Map<String, List<String>> values = new LinkedHashMap<String, List<String>>();
        List<String> columnNames = new LinkedList<String>();
        String[] nodesInfo = new String[GlobalVar.numberOfNodes];
        int nodeCount=0;
        BufferedReader br = null;
        br = new BufferedReader(reader);
        String line;
        int numLines = 0;
        while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) {
                    String[] tokens = line.split(separator);
                    if (tokens != null) {
                            if (numLines == 0) {
                                for (int i = 0; i < tokens.length; ++i) {
                                columnNames.add(hasHeader ? tokens[i] : ("row_"+i));
                                }
                            } else {
                               nodesInfo[nodeCount++]=  tokens[0];                            
                            }
                        
                    }
                    ++numLines;                
            }
        }
        return nodesInfo;
    }	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		GlobalVar.numberOfNodes = Integer.parseInt(stormConf.get("numberOfNodes").toString());
		this.fileName = stormConf.get("wordsFile").toString();
		
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