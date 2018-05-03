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
	
	public class AvgStableRegionReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
				float lowest_average_price = 0, price = 0;
				int i =0;
				
				/*
				 * For each value in the set of values passed to us by the mapper:
				 */
				for (FloatWritable value : values) {
					i++;
					if(i==1) {
						lowest_average_price = value.get();
					}
						
					else {
						price = value.get();
							
						if(price < lowest_average_price) {
							lowest_average_price = price;
						}
					}
				}
				
				/*
				 * Call the write method on the Context object to emit a key
				 * and a value from the reduce method. 
				 */
				context.write(key, new FloatWritable(lowest_average_price));
		}
	}


