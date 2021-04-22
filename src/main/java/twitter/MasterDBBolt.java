package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class MasterDBBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private String tableName;
	private Table table;

	public MasterDBBolt(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		try{
			Configuration conf = HBaseConfiguration.create();
			Connection connection = ConnectionFactory.createConnection(conf);
			this.table = connection.getTable(TableName.valueOf(tableName));
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		String tweetID = (String) tuple.getValueByField("tweet_ID");
		String path_img = (String) tuple.getValueByField("path_img");
		String keyword = (String) tuple.getValueByField("keyword");
		Put newRow = new Put(Bytes.toBytes(tweetID));
		newRow.addColumn(Bytes.toBytes("content"), Bytes.toBytes("path_img"), Bytes.toBytes(path_img));
		newRow.addColumn(Bytes.toBytes("content"), Bytes.toBytes("keyword"), Bytes.toBytes(keyword));
		try{
			table.put(newRow);
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
	}
}
