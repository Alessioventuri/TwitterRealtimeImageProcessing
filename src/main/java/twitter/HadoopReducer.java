package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class HadoopReducer extends Reducer<Center, Point, IntWritable, Center> {
	private Logger logger = Logger.getLogger(HadoopReducer.class);
	private HashMap<IntWritable,Center> newCenters = new HashMap<IntWritable, Center>();
	private HashMap<IntWritable,Center> oldCenters = new HashMap<IntWritable, Center>();
	private int iConvergedCenters = 0;

	public enum CONVERGE_COUNTER {
		CONVERGED
	}

	@Override
	public void reduce(Center key, Iterable<Point> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		Center newCenter = new Center(conf.getInt("iParameters", 144));
		boolean flagOld = false;
		if (newCenters.containsKey(key.getIndex())){
			newCenter = newCenters.get(key.getIndex());
			flagOld = true;
		}

		int numElements = 0;
		Double temp;
		for (Point p : values) {
			for (int i = 0; i < p.getListOfFeatures().size(); i++) {
				temp = newCenter.getListOfFeatures().get(i).get() + p.getListOfFeatures().get(i).get();
				newCenter.getListOfFeatures().get(i).set(temp);
			}
			numElements+= key.getNumberOfPoints().get();

		}
		newCenter.setIndex(key.getIndex());
		newCenter.addNumberOfPoints(new IntWritable(numElements));

		if(!flagOld){
			newCenters.put(newCenter.getIndex(),newCenter);
			oldCenters.put(key.getIndex(),new Center(key));
		}

		context.write(newCenter.getIndex(), newCenter);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path centersPath = new Path(conf.get("centersFilePath"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(centersPath, true);
		SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(centersPath),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Center.class));

		Iterator<Center> it = newCenters.values().iterator();
		Center newCenterValue;
        Center sameIndexC;
        Double avgValue = 0.0;
        Double threshold = conf.getDouble("threshold", 0.5);
        int k = conf.getInt("k", 3);
		while (it.hasNext()){
			newCenterValue = it.next();
			newCenterValue.divideCoordinates();
            sameIndexC = oldCenters.get(newCenterValue.getIndex());
            if (newCenterValue.isConverged(sameIndexC, threshold))
                iConvergedCenters++;

			if (newCenterValue.isConverged(oldCenters.get(newCenterValue.getIndex()), conf.getDouble("threshold", 0.5)))
				iConvergedCenters++;
            avgValue += Math.pow(Distance.findDistance(newCenterValue, sameIndexC), 2);
			centerWriter.append(newCenterValue.getIndex(),newCenterValue);

		}
		avgValue = Math.sqrt(avgValue / k);
        logger.fatal("Convergence value: " + avgValue);
        double percentSize = (newCenters.size() *0.9);
        System.out.println(percentSize);
        if (iConvergedCenters >= percentSize || avgValue < threshold)
            context.getCounter(CONVERGE_COUNTER.CONVERGED).increment(1);
		centerWriter.close();
		logger.fatal("Converged " + iConvergedCenters);
	}

}