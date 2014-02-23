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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class LISABoltOutputJSon implements IRichBolt {

	  private static final long serialVersionUID = 1L;
      private static final String jsonFilePath = "/home/simpal/stormSensorReco/SensorSimulation/SensorSimulations/StormSensorApp/jsonSensorFile.txt";
	  FileWriter jsonFileWriter;

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
		String[] sensorValues = dat.split(",");
		Double lisaVal = input.getDouble(2);
		String value= gid + "," + dat + "," + lisaVal ;
		
		try {
			File file = new File(jsonFilePath);
			jsonFileWriter = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(jsonFileWriter);			
			bw.write(value);
			bw.newLine();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*JSONObject jsonObject = new JSONObject();

		jsonObject.put("groupId", gid);

		jsonObject.put("SensorId", sensorValues[0].toString());

		jsonObject.put("lisaVal", lisaVal);


		try {

			jsonFileWriter.write(jsonObject.toJSONString());
			jsonFileWriter.flush();

			System.out.print("JSon Object:" + jsonObject);

		} catch (IOException e) {
			e.printStackTrace();
		}*/
		//Compute mean of all neighbors
		
		collector.ack(input);
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}