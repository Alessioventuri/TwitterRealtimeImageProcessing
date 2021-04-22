package twitter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DatabaseUtils{

	public static List<Center> getKCentersFromRealTimeTable(String tableName, int k)
			throws IOException {

		int numOfPoints = 0;
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));

		Scan scan = new Scan();

		List<Center> centerVals = new ArrayList<Center>();
		ResultScanner scanner = table.getScanner(scan);
		try {
			for (Result result = scanner.next(); result != null; result = scanner
					.next()) {
				List<DoubleWritable> listOfFeatures = new ArrayList<DoubleWritable>();
				byte[] byteVector  = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("featureVector"));
				String vector = new String(byteVector);
				byte[] bytePath = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("path_img"));
				String path = new String(bytePath);
				String[] vectCleaned= vector.split(",");
				boolean similar = false;

				for (String s : vectCleaned ) {
					s = s.replace("[", "");
					s = s.replace("]", "");
					listOfFeatures.add(new DoubleWritable(Double.parseDouble(s)));
				}
				Center center = new Center(listOfFeatures);
				center.setPath(new Text(path));
				if( centerVals.isEmpty()) {
					centerVals.add(center);
					numOfPoints++;
				}
				similar = center.checkCompare(centerVals);
				if (similar == false) {
					centerVals.add(center);
					numOfPoints++;
				}
				
				if (numOfPoints == k)
					break;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		scanner.close();
		table.close();
		return centerVals;
	}

	public static void putPoints(Configuration conf,String tablename, Integer k) throws IOException {
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tablename));
		List<Center> centers = new ArrayList<Center>();
		Path centersPath = new Path(conf.get("centersFilePath"));  
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
		IntWritable key = new IntWritable();
		Center value = new Center();
		while (reader.next(key, value)) {
			Center c = new Center(value.getListOfFeatures());
			c.setNumberOfPoints(value.getNumberOfPoints());
			c.setIndex(key);
			centers.add(c);
		}
		reader.close();
		for (Center c : centers) {
			Double minDistance = Double.MAX_VALUE;
			Double distanceTemp;
			String nearest = "";
			for (Point p : PointsCounter.points){
				if (c.getIndex().get() == p.getIndexP().get()) {
					distanceTemp = Distance.findDistance(c, p);
					if (minDistance > distanceTemp) {          		
						nearest = p.getPath().toString();
						minDistance = distanceTemp;  
					}
				}
			}
			c.setPath(new Text(nearest));
		}
		for (int i = 0; i < centers.size(); i++) {			
			addRecord(table, "center" + i, "content", "featureVector","nearestPath",centers.get(i));
		}
	}

	public static void addRecord(Table table, String rowKey, String family,
			String qualifier,String qualifier_1, Center c) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier_1),
				Bytes.toBytes(c.getPath().toString()));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
				Bytes.toBytes(Arrays.toString(c.getListOfFeaturesDouble())));
		table.put(put);
	}



}