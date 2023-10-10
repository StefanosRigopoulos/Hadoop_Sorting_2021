import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/* 00:id 
 * 01:age 
 * 02:education 
 * 03:marital_status 
 * 04:income 
 * 05:mntWines;
 */

public class RankMapper extends Mapper <LongWritable, Text, IntWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		
		File file = new File("/home/hdoop/cheats3.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		
		String textInfo = value.toString();
		textInfo = textInfo.replaceAll("\t", "");
		String[] text = textInfo.split(",");
		
		int wineMoney = 0, id = 0, age = 0, income = 0;
		String education = new String();
		String marital_status = new String();
		
		try  {
			id = Integer.parseInt(text[1]);
			age = Integer.parseInt(text[2]);
			education = text[3].toString();
			marital_status = text[4].toString();
			income = Integer.parseInt(text[5]);
			wineMoney = Integer.parseInt(text[6]);
		} catch (NumberFormatException e) {
			id = 0;
			age = 0;
			education = "";
			marital_status = "";
			income = 0;
			wineMoney = 0;
		}
		
		
		// Getting the average
		String line = br.readLine();
		br.close();
		String[] arr = line.split(",");
		int sum = 0, count = 0;
		try {
			sum = Integer.parseInt(arr[0]);
			count = Integer.parseInt(arr[1]);
		} catch (NumberFormatException e) {
			// do nothing
		}
		double average = sum / count;
		
		// Final separation between GOLD and SILVER.
		if (wineMoney >= (average*3)/2) {
			context.write(new IntWritable(wineMoney), new Text("," + id + "," + age + "," + education + "," + marital_status + "," + income + "," + wineMoney));
		}
    }
}
