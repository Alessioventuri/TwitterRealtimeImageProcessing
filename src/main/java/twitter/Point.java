package twitter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nonnull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

public class Point implements Writable{

	private List<DoubleWritable> listOfFeatures;
	private IntWritable indexP;
	private Text img_path;

	Point(List<DoubleWritable> listOfFeatures,Text path){
		this.listOfFeatures = new ArrayList<DoubleWritable>();
		for (DoubleWritable p : listOfFeatures) {
			this.listOfFeatures.add(p);
		}
		this.indexP = new IntWritable();
		this.img_path = path;
	}

	Point(List<DoubleWritable> listOfFeatures){
		this.listOfFeatures = new ArrayList<DoubleWritable>();
		for (DoubleWritable p : listOfFeatures) {
			this.listOfFeatures.add(p);
		}
		this.indexP = new IntWritable();
		this.img_path = new Text("");
	}


	Point(Point p){
		this.indexP = p.getIndexP();
		this.img_path = p.getPath();
		this.listOfFeatures = p.getListOfFeatures();
	}

	public IntWritable getIndexP() {
		return indexP;
	}

	Point(){		
		indexP = new IntWritable();
		img_path = new Text("");
		listOfFeatures= new ArrayList<DoubleWritable>();
	}


	Point(int n) {
		listOfFeatures= new ArrayList<DoubleWritable>();
		for (int i = 0; i < n; i++)
			listOfFeatures.add(new DoubleWritable(0.0));
	}

	Text getPath() {
		return img_path;
	}
	void setPath(Text writable) {
		this.img_path = writable;
	}

	public void readFields(DataInput dataInput) throws IOException {
		int iParams = dataInput.readInt();
		listOfFeatures = new ArrayList<DoubleWritable>();
		for (int i = 0; i < iParams; i++) {
			listOfFeatures.add(new DoubleWritable(dataInput.readDouble()));
		}
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(listOfFeatures.size());
		for (DoubleWritable p : listOfFeatures) {
			dataOutput.writeDouble(p.get());
		}
	}

	public int compareTo(@Nonnull Center c) {
		return 0;
	}

	List<DoubleWritable> getListOfFeatures(){
		return listOfFeatures;
	}
	public void setIndexP(IntWritable index) {
		this.indexP = index;
	}

}
