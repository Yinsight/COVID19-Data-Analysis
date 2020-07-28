import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileReader;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;

public class Covid19_2 extends Configured implements Tool {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text location = new Text();
		IntWritable new_deaths = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String startst = conf.get("start");
			String endst = conf.get("end");

			try {
				if (value.toString().contains("location")
				/*
				 * Some condition satisfying it is header
				 */)
					return;
				else {
					String line = value.toString();
					String[] ParsedLine = line.split(",");
					
					if (ParsedLine[0].compareTo(startst) >= 0
							&& ParsedLine[0].compareTo(endst) <= 0){
						location.set(ParsedLine[1]);
						new_deaths.set(Integer.parseInt(ParsedLine[3]));

						context.write(location, new_deaths);
					}

				}

			}

			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new Covid19_2(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 4) {
			System.out
					.printf("Usage: hadoop Covid19.jar Covid19_1 input YYYY-MM-DD(start date) YYYY-MM-DD(end date) output");
			System.exit(-1);
		}

		String start_date = args[1];
		String end_date = args[2];

		if (start_date.matches("\\d{4}-\\d{2}-\\d{2}") == false || end_date.matches("\\d{4}-\\d{2}-\\d{2}") == false) {
			System.out
			.printf("Usage: date format should be YYYY-MM-DD");
			System.exit(-1);
		}
		
		SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
	    Date d1 = sdformat.parse("2019-12-31");
	    Date d2 = sdformat.parse("2020-04-08");
	    Date start_d = sdformat.parse(start_date);
	    Date end_d = sdformat.parse(end_date);
	    
	    if(start_d.compareTo(d1) < 0) {
	    	System.out
			.printf("Usage: start date must be after (or on) 2019-12-31");
			System.exit(-1);
	      }
	    
	    if(end_d.compareTo(d2) > 0) {
	    	System.out
			.printf("Usage: end date must be before (or on) 2020-04-08");
			System.exit(-1);
	      }
	    
	    if(start_d.compareTo(end_d) > 0) {
	    	System.out
			.printf("Usage: start date must be before end date");
			System.exit(-1);
	      }

		Configuration conf = new Configuration();
		conf.set("start", start_date);
		conf.set("end", end_date);
		Job job = new Job(conf, "Covid19_2");
		job.setJarByClass(Covid19_2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);

	}
}