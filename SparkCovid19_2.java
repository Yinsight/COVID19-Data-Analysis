/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.broadcast.Broadcast;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.String;
import java.lang.Object;
import java.nio.file.Paths;
import java.io.FileReader;
import java.io.File;

public class SparkCovid19_2 {

	/**
	 * args[0]: Input file path on distributed file system args[1]: Output file
	 * path on distributed file system
	 */
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out
					.printf("Usage: hadoop Covid19.jar Covid19_1 input pathtopopulation output");
			System.exit(-1);
		}
		
		String input = args[0];
		String output = args[2];

		final String pathinput = args[1];

		/* essential to run any spark code */
		SparkConf conf = new SparkConf().setAppName("SparkCovid19_2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		BufferedReader fis;
		Hashtable<String, String> locandpopdict = new Hashtable<String, String>();
		File f = new File(pathinput);
		String filesname = f.getName().toString();
		try {
			fis = new BufferedReader(new FileReader(filesname));
			String fileline = "";
			while ((fileline = fis.readLine()) != null) {
				String[] ParsedFileLine = fileline.split(",");
				if (ParsedFileLine.length > 4) {
					locandpopdict.put(ParsedFileLine[1], ParsedFileLine[4]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Broadcast Variable
		final Broadcast<Hashtable<String, String>> broadcastlocandpop = sc
				.broadcast(locandpopdict);

		/* load input data to RDD */
		JavaRDD<String> dataRDD = sc.textFile(args[0]);

		JavaPairRDD<String, Double> counts = dataRDD.flatMapToPair(
				new PairFlatMapFunction<String, String, Double>() {
					public Iterable<Tuple2<String, Double>> call(String value) {

						String line = value.toString();
						String[] ParsedLine = line.split("\n");

						List<Tuple2<String, Double>> retWords = new ArrayList<Tuple2<String, Double>>();

						for (String word : ParsedLine) {
							if (!word.contains("location")
							/*
							 * Some condition satisfying it is header
							 */) {

								String[] ParsedWord = line.split(",");

								if (broadcastlocandpop.value().containsKey(
										ParsedWord[1])) {

									String population = broadcastlocandpop
											.value().get(ParsedWord[1]);
									double population_d = Double
											.parseDouble(population);

									retWords.add(new Tuple2<String, Double>(
											ParsedWord[1],
											(Double.parseDouble(ParsedWord[2]) / population_d) * 1000000));

								}
							}
						}
						return retWords;

					}
				}).reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double x, Double y) {
				return x + y;
			}
		});

		counts.saveAsTextFile(output);

	}
}
