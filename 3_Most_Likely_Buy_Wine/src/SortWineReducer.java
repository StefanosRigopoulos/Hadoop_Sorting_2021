import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortWineReducer extends Reducer <IntWritable, Text, IntWritable, Text> {
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Text output = new Text();
		for(Text val : values) {
			String line  = val.toString();
			output.set(line);
		}
		context.write(key, output);		
	}
}
