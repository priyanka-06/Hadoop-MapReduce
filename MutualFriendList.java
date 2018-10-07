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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriendList {

	public static class Map
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

	public static class Reduce
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

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		// create a job with name "mutualfriendlist"
		Job job = new Job(conf, "mutualfriendlist");
		job.setJarByClass(MutualFriendList.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
