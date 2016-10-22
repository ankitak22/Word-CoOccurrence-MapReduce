import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class CoOccurrenceDescendingReducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value : values) {
				context.write(value, key);
			}
		}
	}