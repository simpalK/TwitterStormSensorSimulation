package spouts;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SensorRealTimeEmitter extends BaseRichSpout{
	private static final long serialVersionUID = 1L;
	  Queue<String> feedQueue = new LinkedList<String>();
	  String[] feeds;
	  private SpoutOutputCollector collector;

	  public SensorRealTimeEmitter(String[] feeds) {
	    this.feeds = feeds;
	  }

	  @Override
	  public void nextTuple() {
	    String nextFeed = feedQueue.poll();
	    if(nextFeed != null) {
	      collector.emit(new Values(nextFeed), nextFeed);
	    }
	  }

	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    for(String feed: feeds) {
	      feedQueue.add(feed);
	    }
	    this.collector = collector;
	  }

	  @Override
	  public void ack(Object feedId) {
	    feedQueue.add((String)feedId);
	  }

	  @Override
	  public void fail(Object feedId) {
	    feedQueue.add((String)feedId);
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("feed"));
	  }

	
}
