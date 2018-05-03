/**
 * 
 */
package solution;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author training
 *
 */
public class LowestPricePerRegionMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	

	/**
	 * @param args
	 */
	@Override
	 public void map(LongWritable key, Text value, Context context) 
		      throws IOException, InterruptedException {
			  							
		 	String[] input_value = value.toString().split("\n");
		 	
		 	for(int i=0;i< input_value.length; i++) {
		 		String[] row = input_value[i].split(",");
		 			String instance_type = row[1] + row[2];
		 			 float instance_price = Float.parseFloat(row[4]);
		 			
		 			context.write(new Text(instance_type), new FloatWritable(instance_price));
		 	}
		 }

}
																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																													