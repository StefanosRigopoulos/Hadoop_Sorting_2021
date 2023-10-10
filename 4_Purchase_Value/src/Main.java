import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static void main(String[] args) throws Exception{
		if(args.length > 4) {
			System.err.println("Give the correct arguements.");
			System.exit(3);
		}
		// Creating an object of Configuration class, which loads the configuration parameters.
		// The Job class allows the user  to configure the job, submit it and control its execution.
		Configuration conf = new Configuration();
		
		// Creating the object of Job class and passing the conf object and Job name as arguments.
		Job job = Job.getInstance(conf, "Count");
		// Setting the jar by finding where a given class came from.
		job.setJarByClass(Main.class);
		// Setting the key class for job output data.
		job.setOutputKeyClass(IntWritable.class);
		// Setting the value class for job output data.
		job.setOutputValueClass(Text.class);
		// Setting the mapper for the job.
		job.setMapperClass(CountMapper.class);
		// Setting the reducer for the job.
		job.setCombinerClass(CountReducer.class);
		job.setReducerClass(CountReducer.class);
		// Adding a path which will act as a input for MR job.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Setting the path to a directory where MR job will dump the  output.
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		// Submitting the job to the cluster and waiting for its completion.
		job.waitForCompletion(true);
		
		
		// Configure the MapReduce job.
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Ranking");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(RankMapper.class);
        job2.setCombinerClass(RankReducer.class);
        job2.setReducerClass(RankReducer.class);
		// Setting the key class for job output data.
		job2.setOutputKeyClass(Text.class);
		// Setting the value class for job output data.
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job2,new Path(args[2]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}