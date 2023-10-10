import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private int count = 1;
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	

		IntWritable rank = new IntWritable();
		rank.set(count);
		count++;
		
		Text output = new Text();
		for(Text val : values) {
			String line  = val.toString();
			output.set(line);
		}
		context.write(rank, output);
    }
}
