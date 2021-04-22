package twitter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Center extends Point implements WritableComparable<Center> {

	private IntWritable index;
	private IntWritable numberOfPoints;

	Center(){
		super();
	}

	Center(int n) {
		super(n);
		setNumberOfPoints(new IntWritable(0));

	}
	Center(Center c) {
		super(c.getListOfFeatures(),c.getPath());
		setNumberOfPoints(c.getNumberOfPoints());
		setIndex(c.getIndex());		

	}

	Center(List<DoubleWritable> list){
		super(list);
		index = new IntWritable(0);
		numberOfPoints = new IntWritable(0);
	}

	Double[] getListOfFeaturesDouble() {
		List<DoubleWritable> list = this.getListOfFeatures();
		Double[] vals = new Double[144];
		int i = 0;
		for (DoubleWritable d : list) {
			vals[i++] = Double.parseDouble(d.toString().replace("[", "").replace("]", ""));
		}
		return vals;
	}

	public void readFields(DataInput dataInput) throws IOException {
		super.readFields(dataInput);
		index = new IntWritable(dataInput.readInt());
		numberOfPoints = new IntWritable(dataInput.readInt());
	}

	public void write(DataOutput dataOutput) throws IOException {
		super.write(dataOutput);
		dataOutput.writeInt(index.get());
		dataOutput.writeInt(numberOfPoints.get());
	}

	@Override
	public int compareTo(@Nonnull Center c) {
		if (this.getListOfFeaturesDouble().equals(c.getListOfFeaturesDouble())) {
			return 0;
		}
		return 1;
	}

	boolean isConverged(Center c, Double threshold) {
		return threshold > Distance.findDistance(this, c);
	}

	public String toString() {
		return this.getIndex() + ";" + super.toString();
	}

	void divideCoordinates() {
		for (int i = 0; i < this.getListOfFeatures().size(); i++) {
			this.getListOfFeatures().set(i, new DoubleWritable(this.getListOfFeatures().get(i).get() / numberOfPoints.get()));
		}
	}

	void addNumberOfPoints(IntWritable i) {
		this.numberOfPoints = new IntWritable(this.numberOfPoints.get() + i.get());
	}

	IntWritable getIndex() {
		return index;
	}

	boolean compare(Center c) {
		boolean equal = false;
		for (int i = 0; i < c.getListOfFeaturesDouble().length; i++) {
			if(c.getListOfFeatures().get(i).get()== this.getListOfFeatures().get(i).get())
				equal = true;
			else
				equal = false;
			if (equal == false)
				return false;
		}
		return true;
	}
	boolean checkCompare(List<Center> c) {
		int counter = 0;
		int counterPoint = 0;
		for (Center center : c){
			for (int i = 0; i < center.getListOfFeaturesDouble().length; i++) {
				if(center.getListOfFeatures().get(i).get()== this.getListOfFeatures().get(i).get()) {
					counter++;
				}
			}
			if (counter == center.getListOfFeaturesDouble().length)
				counterPoint++;
		}
		if (counterPoint == 0)
			return false;
		return true;
	}

	IntWritable getNumberOfPoints() {
		return numberOfPoints;
	}

	void setIndex(IntWritable index) {
		this.index = new IntWritable(index.get());
	}

	void setNumberOfPoints(IntWritable numberOfPoints) {
		this.numberOfPoints = new IntWritable(numberOfPoints.get());
	}

}
