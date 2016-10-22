import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class CoOccurrenceDescendingMapper2 extends Mapper<Text, IntWritable, IntWritable, Text> {
	  public void map(Text key, IntWritable value, Context context)
		      throws IOException, InterruptedException {
		   context.write(value,  key);
			  
			  
		}
	}