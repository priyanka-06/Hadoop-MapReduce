import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DetailedFriendList {

	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String,String[]> map = new HashMap<String,String[]>();
		private final static IntWritable one = new IntWritable(1);
		private Text outputkey = new Text(); // type of output key
		private Text outputvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\t");
			String userId= mydata[0];
			String[] frienddata = null;
			if(mydata.length>1) {
				frienddata = mydata[1].toString().split(",");

				StringBuilder outData = new StringBuilder();
				for(String data1 : frienddata ) {
					if(map.containsKey(data1)) {

						if(outData.toString().equals("")) {
							outData.append(data1+ ":"+map.get(data1)[0]+":"+map.get(data1)[1]);
						}else {
							outData.append(","+data1+ ":"+map.get(data1)[0]+":"+map.get(data1)[1]);
						}	
					}
				}
				outputvalue.set(outData.toString());

				for (String data : frienddata) {
					if(Integer.parseInt(userId) < Integer.parseInt(data)) {
						outputkey.set(userId + ","+data);
					}else {
						outputkey.set(data+","+userId);
					}		
					context.write(outputkey, outputvalue); 
				}
			}		
		}


		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
			Path part=new Path(context.getConfiguration().get("ARGUMENT"));//Location of file in HDFS


			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line=br.readLine();
				while (line != null){
					String[] arr=line.split(",");
					map.put(arr[0], new String[]{arr[1],arr[5]});
					line=br.readLine();
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
				String[] frienddata1 = val.toString().split(",");

				for(String data : frienddata1) {
					String[] frienddata2 = data.toString().split(":");
					if(h.contains(Integer.parseInt(frienddata2[0]))) {
						try {
							if(output.toString().equals("")) {
								output.append("["+frienddata2[1]+":"+frienddata2[2]);
							}else {
								output.append(","+frienddata2[1]+":"+frienddata2[2]);
							}
						}catch(Exception e) {
							System.err.println(data);
						}
					}else {
						h.add(Integer.parseInt(frienddata2[0]));
					}
				}
			}
			if(output.toString().equals("")) {
				output.append("[]");
			}else {
				output.append("]");
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
		if (otherArgs.length != 3) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		conf.set("ARGUMENT",otherArgs[1]);

		// create a job with name "wordcount"
		Job job = new Job(conf, "DetailedFriendList");
		job.setJarByClass(DetailedFriendList.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
