import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortIDMapper extends Mapper <LongWritable, Text, IntWritable, Text> {
	
	private IntWritable idCostumer = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

		String my_new_str = value.toString().replace("\"", "");
		String[] arrOfVal = my_new_str.toString().split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
		try {
			idCostumer.set(Integer.parseInt(arrOfVal[0]));
			context.write(idCostumer, new Text(toString(arrOfVal)));
		} catch (NumberFormatException e) {
			// do nothing
		}
	}
	
	private String toString(String[] a) {
		String fullString = "";
		for (int i = 1; i < 27; i++) {
			fullString += "," + a[i];
		}
		return fullString;
	}
}