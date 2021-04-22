package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HadoopCombiner extends  Reducer<Center, Point, Center,Point>{

	@Override
	public void reduce(Center key, Iterable<Point> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Point sumValues = new Point(conf.getInt("iCoordinates", 144));
		int countValues = 0;
		Double temp;
		for (Point p : values) {
			for (int i = 0; i < p.getListOfFeatures().size(); i++) {
				temp = sumValues.getListOfFeatures().get(i).get() + p.getListOfFeatures().get(i).get();
				sumValues.getListOfFeatures().get(i).set(temp);
			}
			countValues++;
		}
		key.setNumberOfPoints(new IntWritable(countValues));
		context.write(key, sumValues);
	}
}