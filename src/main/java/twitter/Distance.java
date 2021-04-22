package twitter;

import java.util.List;

import org.apache.hadoop.io.DoubleWritable;


class Distance {
	static Double findDistance(Point p1, Point p2) {
		int len = p1.getListOfFeatures().size();
		List<DoubleWritable> l1 = p1.getListOfFeatures();
		List<DoubleWritable> l2 = p2.getListOfFeatures();
		Double sum = 0.0;
		for (int i = 0; i < len; i++) {
			sum += Math.pow(l1.get(i).get() - l2.get(i).get(), 2);
		}
		return Math.sqrt(sum);
	}
}

