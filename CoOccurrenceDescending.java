
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;


	public class CoOccurrenceDescending extends Configured implements Tool {
		  public static void main(String[] args) throws Exception {	  
		    	int exitCode = ToolRunner.run(new Configuration(), new CoOccurrenceDescending(),args);
		    System.exit(exitCode);
		  }

		@Override
		public int run(String[] args) throws Exception {
			 if (args.length != 2) {
			      System.err.println("Usage: MatrixMult <input path> <output path>");
			      System.exit(-1);
			    }
			    //job1 is the mapreduce job for the first step of matrix multiplication
			    Job job1 = new Job(getConf());
			    job1.setJarByClass(CoOccurrenceDescending.class);
			    job1.setJobName("CoOcc");
			    //Create a temporary file to store the result of job1
			    FileInputFormat.addInputPath(job1, new Path(args[0]));
			    Path tempOut = new Path("temp");
			    SequenceFileOutputFormat.setOutputPath(job1, tempOut);
			    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

			    job1.setMapperClass(CoOccurrenceDescendingMapper1.class);
			    job1.setReducerClass(CoOccurrenceDescendingReducer1.class);
			    job1.setMapOutputKeyClass(Text.class);
			    job1.setMapOutputValueClass(IntWritable.class);
			    job1.setOutputKeyClass(Text.class);
			    job1.setOutputValueClass(IntWritable.class);
			    job1.waitForCompletion(true);

			    //Job2 is the mapreduce job for the second step of matrix multiplication
			    Job job2 = new Job();
			    job2.setJarByClass(CoOccurrenceDescending.class);
			    job2.setJobName("Sort");

			    //The input of job2 is the output of job 1
			    job2.setInputFormatClass(SequenceFileInputFormat.class);
			    SequenceFileInputFormat.addInputPath(job2, tempOut);
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			    job2.setMapperClass(CoOccurrenceDescendingMapper2.class);
			    job2.setReducerClass(CoOccurrenceDescendingReducer2.class);
			    job2.setMapOutputKeyClass(IntWritable.class);
			    job2.setMapOutputValueClass(Text.class);
			    job2.setOutputKeyClass(Text.class);
			    job2.setOutputValueClass(IntWritable.class);
			    job2.setSortComparatorClass(DescendingIntWritableComparable.DecreasingComparator.class);
			    job2.waitForCompletion(true);
			    return(job2.waitForCompletion(true) ? 0 : 1);
		}
		
		public static class DescendingIntWritableComparable extends IntWritable {
		    /** A decreasing Comparator optimized for IntWritable. */ 
		    public static class DecreasingComparator extends Comparator {
		        public int compare(WritableComparable a, WritableComparable b) {
		            return -super.compare(a, b);
		        }
		        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		            return -super.compare(b1, s1, l1, b2, s2, l2);
		        }
		    }
	}
	}