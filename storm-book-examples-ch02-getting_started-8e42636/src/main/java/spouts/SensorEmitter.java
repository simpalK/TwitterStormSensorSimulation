package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SensorEmitter implements IRichSpout {

	private SpoutOutputCollector collector;
	private int count = 1000000;  
	private FileReader fileReader;
	private boolean completed = false;
    private File fileSensor1;
    private File fileSensor2;
    private File fileSensor3;
    private File fileSensor4;
    private File fileTopology;

	private String deviceID = "SimpalSensor";
	Random random;
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	public void close() {}
	public void fail(Object msgId) {
		System.out.println("Not Read:"+msgId);
	}

	/**
	 * The only thing that the methods will do It is emit each 
	 * file line
	 */
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have read the file
		 * we will wait and then return
		 */
		if(completed){
			try {
				Thread.sleep(30000);
				completed = false;
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		} 
		//Open the reader
		String fromfileData1 = lastNlines(fileSensor1,400);
		String fromfileData2 = lastNlines(fileSensor2,400);
		String fromfileData3 = lastNlines(fileSensor3,400);
		String fromfileData4 = lastNlines(fileSensor4,400);

		String[] tokens=fromfileData1.split("[\n]");
		
		//System.out.print("data coming from file" + fromfileData1 + "tokens" + tokens);
		try{
			//Read all lines
			/*for(int i=0; i<tokens.length; i++)
			{
				this.collector.emit(new Values(tokens[i]),tokens[i]);
				System.out.print("data coming from 1 token" + tokens[i]);
				
			}*/
			
			this.collector.emit(new Values(fromfileData1 + fromfileData2 + fromfileData3 + fromfileData4),fromfileData1 + fromfileData2 + fromfileData3 + fromfileData4);
			//System.out.print("data coming from 1 token" + fromfileData1 + fromfileData2 + fromfileData3 + fromfileData4);
			//this.collector. .emit(new Values(fromfileData),fromfileData);
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
		
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		//this.fileReader = new FileReader(conf.get("wordsFile").toString());
		String fileName = conf.get("wordsFile").toString();
		this.fileSensor1 = new File(fileName + "1.txt");
		this.fileSensor2 = new File(fileName + "2.txt");
		this.fileSensor3 = new File(fileName + "3.txt");
		this.fileSensor4 = new File(fileName + "4.txt");
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
	public static String lastNlines( File file, int lines) {
	    java.io.RandomAccessFile fileHandler = null;
	    try {
	        fileHandler = 
	        new java.io.RandomAccessFile( file, "r" );
	        long fileLength = file.length() - 1;
	        StringBuilder sb = new StringBuilder();
	        int line = 0;

	        for(long filePointer = fileLength; filePointer != -1; filePointer--){
	            fileHandler.seek( filePointer );
	            int readByte = fileHandler.readByte();

	            if( readByte == 0xA ) {
	                if (line == lines) {
	                    if (filePointer == fileLength) {
	                        continue;
	                    } else {
	                        break;
	                    }
	                }
	            } else if( readByte == 0xD ) {
	                line = line + 1;
	                if (line == lines) {
	                    if (filePointer == fileLength - 1) {
	                        continue;
	                    } else {
	                        break;
	                    }
	                }
	            }
	           sb.append( ( char ) readByte );
	        }

	        sb.deleteCharAt(sb.length()-1);
	        String lastLine = sb.reverse().toString();
	        return lastLine;
	    } catch( java.io.FileNotFoundException e ) {
	        e.printStackTrace();
	        return null;
	    } catch( java.io.IOException e ) {
	        e.printStackTrace();
	        return null;
	    }
	     finally {
	        if (fileHandler != null )
	            try {
	                fileHandler.close();
	            } catch (IOException e) {
	                /* ignore */
	            }
	    }
	}
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
