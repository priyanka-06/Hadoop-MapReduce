import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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


public class MinimumAge {

	public static class FriendMapper1
	extends Mapper<LongWritable, Text, Text, Text>{

		private Text outputkey = new Text(); // type of output key
		private Text outputvalue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
			String userId= mydata[0];
			if(mydata.length>1) {
				outputkey.set(userId);
				outputvalue.set(mydata[1]);
				context.write(outputkey, outputvalue);
			}			
		}
	}

	public static class UserMapper2
	extends Mapper<LongWritable, Text, Text, Text>{

		private Text outputkey = new Text(); // type of output key
		private Text outputvalue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split(",");
			String userId= mydata[0];

			try {
				DateFormat format = new SimpleDateFormat("mm/dd/yyyy", Locale.ENGLISH);
				Date dob=format.parse(mydata[9]);
				Calendar today = Calendar.getInstance();
				Calendar dateOfBirth = Calendar.getInstance();
				dateOfBirth.setTime(dob);

				int age = today.get(Calendar.YEAR)-dateOfBirth.get(Calendar.YEAR)-1;
				if (today.get(Calendar.DAY_OF_YEAR)>=dateOfBirth.get(Calendar.DAY_OF_YEAR)) {
					age++;
				}

				String outdata="Second Mapper,"+mydata[3]+","+mydata[4]+","+mydata[5]+","+age;

				outputkey.set(userId);
				outputvalue.set(outdata);

				context.write(outputkey, outputvalue);

			} catch (ParseException e) {
				e.printStackTrace();
			}

		}
	}

	public static class Reduce
	extends Reducer<Text,Text,Text,Text> {

		private Text outputkey = new Text(); // type of output key
		private Text outputvalue = new Text();
		HashMap<String, String> friendlist =new HashMap<>();
		HashMap<String,String> address = new HashMap<>();
		HashMap<String, Integer> age= new HashMap<>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


			for (Text val : values) {
				String[] frienddata = val.toString().split(",");
				if(!frienddata[0].equals("Second Mapper")){
					friendlist.put(key.toString(), val.toString());
				}else {
					address.put(key.toString(), frienddata[1]+","+frienddata[2]+","+frienddata[3]);
					age.put(key.toString(), Integer.parseInt(frienddata[4]));
				}
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);

			for(String key: friendlist.keySet()) {
				int minAge= Integer.MAX_VALUE;
				String[] values = friendlist.get(key).split(",");

				for(String val : values) {
					if(age.containsKey(val)) {
						if(minAge > age.get(val)) {
							minAge = age.get(val);
						}
					}
				}
				outputkey.set(key);
				outputvalue.set(address.get(key)+":"+minAge );

				context.write(outputkey, outputvalue);
			}

		}
	}

	public static class MinimumAgeMapper3 extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable frequency = new LongWritable();
		private Text outputvalue = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] countFriends = value.toString().split(":");
			int newVal = Integer.parseInt(countFriends[1]);		   
			String output =key +":"+countFriends[0];
			outputvalue.set(output);
			frequency.set(newVal);
			context.write(frequency, outputvalue);
		}
	}

	//output the top 10 words frequency 
	public static class MinimumAgeReducer2 extends Reducer<LongWritable, Text, Text, Text> {
		private int idx = 0;
		private Text outputkey = new Text(); // type of output key
		private Text outputvalue = new Text();

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				if (idx < 10) {
					idx++;
					String[] data = value.toString().split(":");
					outputkey.set(data[0]);
					outputvalue.set(data[1]+","+key);
					context.write(outputkey, outputvalue);
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: Minimum Age <in1> <in2> <tempout> <out>");
			System.exit(2);
		}

		// create a job with name "MinimumAge"
		Job job = new Job(conf, "reducejoin");
		job.setJarByClass(MinimumAge.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		//set the HDFS path of the input data
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,FriendMapper1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,UserMapper2.class);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//Wait till job completion
		if(!job.waitForCompletion(true))
			System.exit(1);

		{
			conf = new Configuration();
			Job job2 = new Job(conf, "output");

			job2.setJarByClass(MinimumAge.class);
			job2.setMapperClass(MinimumAge.MinimumAgeMapper3.class);
			job2.setReducerClass(MinimumAge.MinimumAgeReducer2.class);

			//set job2's mapper output key type
			job2.setMapOutputKeyClass(LongWritable.class);
			//set job2's mapper output value type
			job2.setMapOutputValueClass(Text.class);

			//set job2's output key type
			job2.setOutputKeyClass(Text.class);
			//set job2's output value type
			job2.setOutputValueClass(Text.class);

			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			//hadoop by default sorts the output of map by key in ascending order, set it to decreasing order
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			job2.setNumReduceTasks(1);
			//job2's input is job1's output
			FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
			//set job2's output path
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}