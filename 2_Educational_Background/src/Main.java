import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length > 3) {
			System.err.println("Give the correct arguements.");
			System.exit(3);
		}
		// Creating an object of Configuration class, which loads the configuration parameters.
		// The Job class allows the user  to configure the job, submit it and control its execution.
		Configuration conf = new Configuration();
		
		// Creating the object of Job class and passing the conf object and Job name as arguments.
		Job job = Job.getInstance(conf, "count");
		// Setting the jar by finding where a given class came from.
		job.setJarByClass(Main.class);
		// Setting the key class for job output data.
		job.setOutputKeyClass(Text.class);
		// Setting the value class for job output data.
		job.setOutputValueClass(IntWritable.class);
		// Setting the mapper for the job.
		job.setMapperClass(FindEbMapper.class);
		// Setting the reducer for the job.
		job.setCombinerClass(FindEbReducer.class);
		job.setReducerClass(FindEbReducer.class);
		// Adding a path which will act as a input for MR job.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Setting the path to a directory where MR job will dump the  output.
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		// Submitting the job to the cluster and waiting for its completion.
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
