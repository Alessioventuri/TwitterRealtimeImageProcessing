package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception{

		String PARAMETERS = "144";
		String NCENTER = "3";
		Double THRESHOLD = 0.01;
		String PathCenter = "/home/alessio/c.seq";
		String Output = "/home/alessio/output/";

		System.out.println("Starting MapReduce");

		BasicConfigurator.configure();
		Configuration conf = new Configuration();

		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf("synchronization_table"));

		table.put(new Put(Bytes.toBytes("MapReduce_start_timestamp"))
				.addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));

		long start = System.currentTimeMillis();
		conf.setLong("start", start);
		Path centers = new Path(PathCenter);

		//-------------------------
		Job job;;
		conf.set("centersFilePath", PathCenter);
		conf.setDouble("threshold", THRESHOLD);

		int k = Integer.parseInt(NCENTER); 
		conf.setInt("k", k);
		int iParameters = Integer.parseInt(PARAMETERS);  
		conf.setInt("iParameters", iParameters);


		System.out.println("Parametri: " + conf.getInt("iParameters",iParameters));

		createCenters(k, conf, centers,"tweet_batch_view");

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("content"));
		scan.setCaching(500);   //Should always set high
		scan.setCacheBlocks(false); // 
		Path output = new Path(Output);
		FileSystem fs = FileSystem.get(output.toUri(),conf);
		if (fs.exists(output)) {
			System.out.println("Delete old output folder: " + output.toString());
			fs.delete(output, true);
		}
		long isConverged = 0;
		int iterations = 0;
		while (isConverged != 1 || iterations == 20) {
			job = Job.getInstance(conf, "BatchTwitterKMeans Iter");
			job.setJarByClass(twitter.HadoopDriver.class);
			TableMapReduceUtil.initTableMapperJob("tweet_master_database", scan, twitter.HadoopMapper.class, Center.class, Point.class, job);
			job.setCombinerClass(HadoopCombiner.class);
			job.setReducerClass(HadoopReducer.class);
			job.setMapOutputKeyClass(Center.class);
			job.setMapOutputValueClass(Point.class);
			FileOutputFormat.setOutputPath(job, output);
			job.waitForCompletion(true);	
			isConverged = job.getCounters().findCounter(HadoopReducer.CONVERGE_COUNTER.CONVERGED).getValue();

			fs.delete(output, true);
			iterations++;
			DatabaseUtils.putPoints(conf,"tweet_batch_view", k);

		}

		job = Job.getInstance(conf, "BatchTwitterKMeans Map");
		PointsCounter.points = new ArrayList<Point>();
		job.setJarByClass(twitter.HadoopDriver.class);
		TableMapReduceUtil.initTableMapperJob("tweet_master_database", scan, twitter.HadoopMapper.class, Center.class, Point.class, job);
		job.setMapOutputKeyClass(Center.class);
		job.setMapOutputValueClass(Point.class);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);	

		fs.delete(output, true);
		DatabaseUtils.putPoints(conf,"tweet_batch_view", k);


		System.out.println("Number of iterations\t" + iterations);
		System.out.println("\n\nStart: " + start + "\n\n");

		int returnValue = job.waitForCompletion(true) ? 0 : 1;

		long end = System.currentTimeMillis();

		System.out.println("\n\nEnd: " + end);
		long elapsed = end - start;
		System.out.println("\n\nElapsed: " + elapsed);
		System.out.println(PointsCounter.points.size());
		table.put(new Put(Bytes.toBytes("MapReduce_end_timestamp"))
				.addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));

		return returnValue;
	}

	private static void createCenters(int k, Configuration conf, Path centers,String tablename) throws IOException {
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tablename));
		SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(centers),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Center.class));
		List<Center> kCenters = DatabaseUtils.getKCentersFromRealTimeTable("tweet_realtime_database", k);
		Center tempC;
		int i = 0;
		for( Center center : kCenters) {
			tempC = new Center(center);
			centerWriter.append(new IntWritable(i), tempC);
			DatabaseUtils.addRecord(table, "center" + i, "content", "featureVector","nearestPath", tempC); 
			i++;
		}
		centerWriter.close();


	}
	public static void main(String[] args) throws Exception{
		while(true){
			ToolRunner.run(new HadoopDriver(), args);
			Thread.sleep(30 * 1000);
			System.exit(0);

		}
	}
}