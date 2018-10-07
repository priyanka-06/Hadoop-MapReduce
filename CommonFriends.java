import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonFriends {

	public static class FriendMapper1
	extends Mapper<LongWritable, Text, Text, Text>{

		private Text outputkey = new Text(); // type of output key
		private Text outputvalue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
			String userId= mydata[0];
			String[] frienddata = null;
			if(mydata.length>1) {
				frienddata = mydata[1].toString().split(",");
				
				for (String data : frienddata) {
					if(Integer.parseInt(userId) < Integer.parseInt(data)) {
						outputkey.set(userId + ","+data);
					}else {
						outputkey.set(data+","+userId);
					}		
					outputvalue.set(mydata[1]);
					context.write(outputkey, outputvalue); 
				}
			}		
		}
	}

	public static class FriendReducer1
	extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<Integer> h= new HashSet<Integer>();
			StringBuilder output = new StringBuilder();
			for (Text val : values) {
				String[] frienddata = val.toString().split(",");
				for(String data : frienddata) {
					
					if(h.contains(Integer.parseInt(data))) {
						if(output.toString().equals("")) {
							output.append(data);
						}else {
							output.append(","+data);
						}			
					}else {
						h.add(Integer.parseInt(data));
					}
				}
			}
			result.set(output.toString());
			context.write(key, result); // create a pair <keyword, number of occurences>
		}
	}
	
	//job2's mapper swap key and value, sort by key (the frequency of each word).
		public static class FriendMapper2 extends Mapper<Text, Text, LongWritable, Text> {

			  private LongWritable frequency = new LongWritable();

			  public void map(Text key, Text value, Context context)
			    throws IOException, InterruptedException {
				String[] countFriends = value.toString().split(",");
			    int newVal = countFriends.length;
			    frequency.set(newVal);
			    context.write(frequency, key);
			  }
			}
		
		//output the top 10 words frequency 
		public static class FriendReducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {
			private int idx = 0;
			 
			public void reduce(LongWritable key, Iterable<Text> values, Context context)
			      throws IOException, InterruptedException {
				
				for (Text value : values) {
			    	if (idx < 10) {
			    		idx++;
			    		context.write(value, key);
			    	}
			    }
			  }
			}

	// Driver program
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        String inputPath = otherArgs[0];
	        String outputPath = otherArgs[1];
	        String tempPath = otherArgs[2];
	        //First Job
	        {	//create first job
	            conf = new Configuration();
	            Job job = new Job(conf, "FriendMapper1");

	            job.setJarByClass(CommonFriends.class);
	            job.setMapperClass(CommonFriends.FriendMapper1.class);
	            job.setReducerClass(CommonFriends.FriendReducer1.class);
	            
	            //set job1's mapper output key type
	            job.setMapOutputKeyClass(Text.class);
	            //set job1's mapper output value type
	            job.setMapOutputValueClass(Text.class);
	            
	            // set job1;s output key type
	            job.setOutputKeyClass(Text.class);
	            // set job1's output value type
	            job.setOutputValueClass(Text.class);
	            //set job1's input HDFS path
	            FileInputFormat.addInputPath(job, new Path(inputPath));
	            //job1's output path
	            FileOutputFormat.setOutputPath(job, new Path(tempPath));

	            if(!job.waitForCompletion(true))
	                System.exit(1);
	        }
		
		 //Second Job
        {
            conf = new Configuration();
            Job job2 = new Job(conf, "FriendMapper2");

            job2.setJarByClass(CommonFriends.class);
            job2.setMapperClass(CommonFriends.FriendMapper2.class);
            job2.setReducerClass(CommonFriends.FriendReducer2.class);
            
            //set job2's mapper output key type
            job2.setMapOutputKeyClass(LongWritable.class);
            //set job2's mapper output value type
            job2.setMapOutputValueClass(Text.class);
            
            //set job2's output key type
            job2.setOutputKeyClass(Text.class);
            //set job2's output value type
            job2.setOutputValueClass(LongWritable.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            
            //hadoop by default sorts the output of map by key in ascending order, set it to decreasing order
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setNumReduceTasks(1);
            //job2's input is job1's output
            FileInputFormat.addInputPath(job2, new Path(tempPath));
            //set job2's output path
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
	}
}