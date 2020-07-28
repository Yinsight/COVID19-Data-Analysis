import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.String;
import java.lang.Object;

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
import java.net.URI;
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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.hadoop.io.LongWritable;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;

public class Covid19_3 extends Configured implements Tool {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		private Text location = new Text();
		double new_cases;
		DoubleWritable new_cases_calc = new DoubleWritable();
		Hashtable<String, String> locandpopdict = new Hashtable<String, String>();
		private BufferedReader fis;

		public void setup(Context context) throws IOException,
				InterruptedException {
			try {
				URI[] files = context.getCacheFiles();

				for (URI file : files) {

					Path filespath = new Path(file.getPath());

					String filesname = filespath.getName().toString();

					try {
						fis = new BufferedReader(new FileReader(filesname));

						String fileline = "";

						while ((fileline = fis.readLine()) != null) {

							String[] ParsedFileLine = fileline.split(",");

							if (ParsedFileLine.length > 4) {

								locandpopdict.put(ParsedFileLine[1],
										ParsedFileLine[4]);

							}
						}

					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				if (value.toString().contains("location")
				/*
				 * Some condition satisfying it is header
				 */)
					return;
				else {

					String line = value.toString();
					String[] ParsedLine = line.split(",");

					location.set(ParsedLine[1]);
					new_cases = Double.parseDouble(ParsedLine[2]);

					String location_st = location.toString();

					if (locandpopdict.containsKey(location_st)) {

						String population = locandpopdict.get(location_st);			
						double population_d = Double.parseDouble(population);
												
						new_cases_calc.set((new_cases/population_d)*1000000);				
						context.write(location, new_cases_calc);

					}
				}
			}

			catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new Covid19_3(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out
					.printf("Usage: hadoop Covid19.jar Covid19_1 input pathtopopulation output");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Covid19_3");
		// job.addCacheFile(new Path("hdfs://quickstart.cloudera:8020/user/root/populations.csv").toUri());
		job.addCacheFile(new Path(args[1]).toUri());
		job.setJarByClass(Covid19_3.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);

	}
}