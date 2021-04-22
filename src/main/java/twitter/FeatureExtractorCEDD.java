package twitter;

import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.DoubleWritable;

import net.semanticmetadata.lire.imageanalysis.features.global.CEDD;

public class FeatureExtractorCEDD {

	private CEDD ceddDescriptor;
	
	public List<DoubleWritable> getDWFeatureVector(String path_img) throws FileNotFoundException, IOException {

		List<DoubleWritable> FV = new ArrayList<DoubleWritable>();
		try {
			BufferedImage img = ImageIO.read(new FileInputStream(path_img));
			ceddDescriptor = new CEDD();
			ceddDescriptor.extract(img);  
			String featureString = Arrays.toString(ceddDescriptor.getFeatureVector());
			List<String> featureDouble = new ArrayList<String>(Arrays.asList(featureString.split(",")));

			for ( String str : featureDouble) {
				str = str.replace("[","");
				str = str.replace("]","");
				FV.add(new DoubleWritable(Double.parseDouble(str)));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return FV;
	}
	
	public String getStringFeatureVector(String path_img) throws FileNotFoundException, IOException {

		String featureString = "";
		try {
			BufferedImage img = ImageIO.read(new FileInputStream(path_img));
			ceddDescriptor = new CEDD();
			ceddDescriptor.extract(img);  
			featureString = Arrays.toString(ceddDescriptor.getFeatureVector());
			featureString.replace("[","");
			featureString.replace("]","");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return featureString;
	}
}
