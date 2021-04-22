	package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HadoopMapper extends TableMapper<Center, Point>{

	private List<Center> centers = new ArrayList<Center>();
	private FeatureExtractorCEDD extractor;
	private long startTimestamp;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException{

		Configuration conf = context.getConfiguration();
		startTimestamp = Long.parseLong(conf.get("start"));
		extractor = new FeatureExtractorCEDD();
		Path centersPath = new Path(conf.get("centersFilePath"));  
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
		IntWritable key = new IntWritable();
		Center value = new Center();
		while (reader.next(key, value)) {
			Center c = new Center(value.getListOfFeatures());
			c.setNumberOfPoints(new IntWritable(0));
			c.setIndex(key);
			centers.add(c);
		}
		reader.close();
	}

	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException{
		long check = value.rawCells()[0].getTimestamp();
		if(check <= startTimestamp){
			byte[] bytePath = value.getValue(Bytes.toBytes("content"), Bytes.toBytes("path_img"));
			String path = new String(bytePath);            
			List<DoubleWritable> d = extractor.getDWFeatureVector(path);
			Point point = new Point(d,new Text(path));
			Center minDistanceCenter = null;
			Double minDistance = Double.MAX_VALUE;
			Double distanceTemp;
			for (Center center: centers) {
				distanceTemp = Distance.findDistance(center,point);
				if (minDistance > distanceTemp) {          		
					minDistanceCenter = new Center(center);
					minDistance = distanceTemp;
				}
			}
			point.setIndexP(minDistanceCenter.getIndex());
			if (!PointsCounter.points.contains(point))
				PointsCounter.points.add(point);

			context.write(minDistanceCenter,point);

		}

	}
}
