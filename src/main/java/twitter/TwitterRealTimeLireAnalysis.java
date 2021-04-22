package twitter;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TwitterRealTimeLireAnalysis{

	public static void main(String[] args) throws Exception{

		List<String> lines = Files.readLines(new File("/home/alessio/twitterCredential.txt"), Charset.defaultCharset());
		System.out.println(lines);
		String consumerKey = Arrays.asList(lines.get(0).split(":")).get(1).trim();
		String consumerSecret = Arrays.asList(lines.get(1).split(":")).get(1).trim();
		String accessToken = Arrays.asList(lines.get(2).split(":")).get(1).trim();
		String accessTokenSecret = Arrays.asList(lines.get(3).split(":")).get(1).trim();

		//Read keywords as an argument
		//    	String[] arguments = args.clone();
		//    	String[] keywords = Arrays.copyOfRange(arguments, 0, arguments.length);
		List<String> arguments = new ArrayList<String>();
		arguments.add("dogs");
		String keyword = arguments.get(0);
		System.out.println(keyword);
		System.out.println("----------------------");

		Config config = new Config();
		config.setDebug(true);

		//HBase tables creation
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		if(!admin.tableExists(TableName.valueOf("tweet_master_database"))){
			TableDescriptor tweetMasterDatabase = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_master_database"))
					.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
					.build();
			admin.createTable(tweetMasterDatabase);
		}

		if(!admin.tableExists(TableName.valueOf("tweet_batch_view"))){
			TableDescriptor tweetBatchView = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_batch_view"))
					.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
					.build();
			admin.createTable(tweetBatchView);
		}

		if(!admin.tableExists(TableName.valueOf("synchronization_table"))){
			TableDescriptor tweetRealtimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("synchronization_table"))
					.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("placeholder".getBytes()).build())
					.build();
			admin.createTable(tweetRealtimeView);
			Table table = connection.getTable(TableName.valueOf("synchronization_table"));
			table.put(new Put(Bytes.toBytes("MapReduce_start_timestamp"), 0)
					.addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));
			table.put(new Put(Bytes.toBytes("MapReduce_end_timestamp"), 0)
					.addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));
		}

		if(!admin.tableExists(TableName.valueOf("tweet_realtime_database"))){
			TableDescriptor tweetRealtimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_realtime_database"))
					.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
					.build();
			admin.createTable(tweetRealtimeView);
		}  

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("twitter-spout", new TwitterSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret, keyword));

		builder.setBolt("twitter-bolt", new TwitterBolt(keyword))
		.shuffleGrouping("twitter-spout");

		builder.setBolt("master-database-mapper-bolt", new MasterDBBolt("tweet_master_database"))
		.shuffleGrouping("twitter-bolt");

		builder.setBolt("twitter-feature-extractor-bolt", new FeatureExtractorCEDDBolt())
		.shuffleGrouping("twitter-bolt");

		builder.setBolt("realtime-database-mapper-bolt", new RealTimeDBMapperBolt("tweet_realtime_database"))
		.shuffleGrouping("twitter-feature-extractor-bolt");

		builder.setSpout("synchronization-spout", new SynchronizationSpout("synchronization_table"));

		builder.setBolt("synchronization-bolt", new SynchronizationBolt("tweet_realtime_database"))
		.shuffleGrouping("synchronization-spout");

		@SuppressWarnings("resource")
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TwitterSentimentAnalysisStorm", config,
				builder.createTopology());

		Thread.sleep(1200000);

	}
}
