package twitter;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.MediaEntity;
import twitter4j.Status;

public class ImageWorker {
	private File output = null;
	private static final Pattern fileExtensionPattern = Pattern.compile(".*(\\.\\w+)");
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("EEE-HH-mm");

	public ImageWorker(final String destination) {
		this.output = new File(destination);
		output.mkdirs();
	}
	public String processTweet(Status tweet) {
		if (tweet.getMediaEntities().length == 0) 
			return "";     
		else {
			String path = processMedia(tweet,tweet.getMediaEntities()[0]);
			return path;
		}
	}

	private String processMedia(Status tweet, MediaEntity entity){
		final String url = entity.getMediaURL();
		final String fileName = dateFormatter.format(tweet.getCreatedAt())+ "-@" + tweet.getUser().getScreenName()  + fileExtensionFor(url);
		final File out = new File(output, fileName);
		System.out.println("Downloading: " + url + " to " + out.getAbsolutePath());

		try {
			URL imageUrl = new URL(url);
			ReadableByteChannel rbc = Channels.newChannel(imageUrl.openStream());
			FileOutputStream fos = new FileOutputStream(out);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			BasicFileAttributeView attributes = Files.getFileAttributeView(Paths.get(out.getAbsolutePath()), BasicFileAttributeView.class);
			FileTime time = FileTime.fromMillis(tweet.getCreatedAt().getTime());
			attributes.setTimes(time, time, time);
			fos.close();


		} catch (Exception e) {
			System.err.println("Could not download: " + url);
			e.printStackTrace();
		}
		return out.getAbsolutePath();
	}

	private String fileExtensionFor(String url) {
		final Matcher matcher = fileExtensionPattern.matcher(url);
		if (matcher.matches()) {
			return matcher.group(matcher.groupCount());
		}
		return ".png";
	}	
}
