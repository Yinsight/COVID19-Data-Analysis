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

public class Covid19_1 extends Configured implements Tool {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text location = new Text();
		IntWritable new_cases = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String choicest = conf.get("choice");

			if (choicest == "false" || choicest == "False") {
				try {
					if (value.toString().contains("location")
							|| value.toString().contains("2019-")
							|| value.toString().contains("World")
							|| value.toString().contains("International")/*
																		 * Some
																		 * condition
																		 * satisfying
																		 * it is
																		 * header
																		 * and
																		 * invalid
																		 * year
																		 */)
						return;
					else {
						String line = value.toString();
						String[] ParsedLine = line.split(",");

						location.set(ParsedLine[1]);
						new_cases.set(Integer.parseInt(ParsedLine[2]));

						context.write(location, new_cases);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else if (choicest == "true" || choicest == "True") {

				try {
					if (value.toString().contains("location")
							|| value.toString().contains("2019-")/*
																 * Some
																 * condition
																 * satisfying it
																 * is header and
																 * invalid year
																 */)
						return;
					else {
						String line = value.toString();
						String[] ParsedLine = line.split(",");

						location.set(ParsedLine[1]);
						new_cases.set(Integer.parseInt(ParsedLine[2]));

						context.write(location, new_cases);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

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
		int res = ToolRunner.run(conf, new Covid19_1(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.printf("Usage: hadoop Covid19.jar Covid19_1 input true/false output");
			System.exit(-1);
		}
		String worldchoice = args[1];

		if (!"true".equals(worldchoice) && !"True".equals(worldchoice)
				&& !"false".equals(worldchoice) && !"False".equals(worldchoice)) {
			System.out
					.printf("Usage: hadoop Covid19.jar Covid19_1 input true/false output");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.set("choice", worldchoice);
		Job job = new Job(conf, "Covid19_1");
		job.setJarByClass(Covid19_1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);

	}
}
