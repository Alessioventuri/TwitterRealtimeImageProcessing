package twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

import java.util.Map;

public class TwitterBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	private String output = "/home/alessio/Desktop/output";
	private String keyword;
	private OutputCollector collector;
	private twitter.ImageWorker imgWorker;

	public TwitterBolt(String keyword) {
		this.keyword = keyword;
	}

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		imgWorker = new twitter.ImageWorker(output);
	}

	@Override
	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		String path_img = imgWorker.processTweet(tweet);
		if (path_img.equals(""))
			return;
		else 
			collector.emit(new Values(String.valueOf(tweet.getId()),keyword,path_img));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_ID","keyword","path_img"));
	}


}
