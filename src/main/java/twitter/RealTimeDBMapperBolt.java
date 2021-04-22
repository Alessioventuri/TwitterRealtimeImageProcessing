package twitter;


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
import java.util.concurrent.ThreadLocalRandom;

public class RealTimeDBMapperBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private String tableName;
	private Table table;

	public RealTimeDBMapperBolt(String tableName) {
		this.tableName = tableName;
	}
	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		try {
			Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
			this.table = connection.getTable(TableName.valueOf(tableName));
		}catch(IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		String keyword = (String) input.getValueByField("keyword");
		String featureVector = (String) input.getValueByField("featureVector");
		String path_img = (String) input.getValueByField("path_img");
		String rowKey = String.valueOf(System.currentTimeMillis()) + " - " + String.valueOf(ThreadLocalRandom.current().nextLong(0,100000));
		Put put = new Put(Bytes.toBytes(rowKey))
				.addColumn(Bytes.toBytes("content"), Bytes.toBytes("keyword"), Bytes.toBytes(keyword))
				.addColumn(Bytes.toBytes("content"), Bytes.toBytes("path_img"), Bytes.toBytes(path_img))
				.addColumn(Bytes.toBytes("content"), Bytes.toBytes("featureVector"), Bytes.toBytes(featureVector.toString()));
		try {
			table.put(put);
		}catch(IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
	}

}
