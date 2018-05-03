package solution;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



	/**
	 * @param args
	 */
	
	public class AveragePricePerRegionReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
				float Totalprice = 0;
				int instance_count = 0;
				float average_price = 0;
			
				/*
				 * For each value in the set of values passed to us by the mapper:
				 */
				for (FloatWritable value : values) {
					
					instance_count++;
				  /*
				   * Add the value to the word count counter for this key.
				   */
					Totalprice += value.get();
				}
				
				average_price = Totalprice/instance_count;
				
				/*
				 * Call the write method on the Context object to emit a key
				 * and a value from the reduce method. 
				 */
				context.write(key, new FloatWritable(average_price));
		}
	}


