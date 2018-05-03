/**
 * 
 */
package solution;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author training
 *
 */
public class LowestPricePerRegion {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		/*
	     * Validate that two arguments were passed from the command line.
	     */
	    if (args.length != 2) {
	      System.out.printf("Usage: AvgWordLength <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Job job = new Job();
	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running 
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(LowestPricePerRegion.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("Average Word Length");

	    /*
	     * TODO implement
	     */
	    Path in = new Path(args[0]);
	    Path out = new Path(args[1]);
	    FileInputFormat.setInputPaths(job, in);
	    FileOutputFormat.setOutputPath(job, out);
	    
	    job.setMapperClass(LowestPricePerRegionMapper.class);
	    job.setReducerClass(LowestPricePerRegionReducer.class);
	    
	    
	    /*
	     * Specify the job's output key and value classes.
	     */																																															
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(FloatWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	}
}
