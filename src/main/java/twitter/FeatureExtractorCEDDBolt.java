package twitter;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FeatureExtractorCEDDBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private FeatureExtractorCEDD extractor;	
	private String featureVector;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		extractor = new FeatureExtractorCEDD();

	}

	@Override
	public void execute(Tuple input) {
		String path_img = (String) input.getValueByField("path_img");
		String keyword = (String) input.getValueByField("keyword");
		try {
			featureVector = extractor.getStringFeatureVector(path_img);
		} catch (IOException e) {
			e.printStackTrace();
		}
		collector.emit(new Values(keyword,path_img,featureVector));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("keyword","path_img", "featureVector"));

	}

}
