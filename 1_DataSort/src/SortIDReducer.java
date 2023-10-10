import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SortIDReducer extends Reducer <IntWritable, Text, IntWritable, Text> {
	
	public void reduce(IntWritable id, Iterable<Text> fullStr, Context context) throws IOException, InterruptedException {
		
		Text output = new Text();
		for(Text val : fullStr) {
			String line  = val.toString();
			output.set(line);
		}
		context.write(id, output);
	}
}