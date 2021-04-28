package twitter;

import java.awt.Desktop;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



class ShowGeneratedHtml {

    public static void show(String tableName ) throws Exception {
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));

        File f = new File("Centers.html");
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write("<html><body>");
		Scan scan = new Scan();
		int i = 0;
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner
				.next()) {
			i++;
			byte[] byteVector  = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("numberOfPoints"));
			String vector = new String(byteVector);
			int numberOfPoints = Integer.valueOf(vector);
			byte[] bytePath = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("nearestPath"));
			String path = new String(bytePath);
        	bw.write("<h1>Center #"+ i +"</h1>");
        	bw.write("<p>");
        	bw.write("Nearest Image path : " + path);
        	bw.write("<br>");
        	bw.write("Number of Points : " + numberOfPoints +" <br>"); 
        	bw.write("<img src="+path+ " "+ "width=" + "250" + "height=" + "300>");
        	bw.write("</p>");

		}
		scanner.close();
		table.close();
        bw.write("</body></html>");
        bw.close();

        Desktop.getDesktop().browse(f.toURI());
    }
}