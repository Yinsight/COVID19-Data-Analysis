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

public class SparkCovid19_1 {

	/**
	 * args[0]: Input file path on distributed file system args[1]: Output file
	 * path on distributed file system
	 */
	public static void main(String[] args) {

		if (args.length != 4) {
			System.out
					.printf("Usage: spark-submit --class Covid19_2 SparkCovid19.jar input YYYY-MM-DD(start date) YYYY-MM-DD(end date) output");
			System.exit(-1);
		}

		String input = args[0];
		String output = args[3];

		final String start_date = args[1];
		final String end_date = args[2];

		if (start_date.matches("\\d{4}-\\d{2}-\\d{2}") == false
				|| end_date.matches("\\d{4}-\\d{2}-\\d{2}") == false) {
			System.out.printf("Usage: data format should be YYYY-MM-DD");
			System.exit(-1);
		}

		try {

			SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
			Date d1 = sdformat.parse("2019-12-31");
			Date d2 = sdformat.parse("2020-04-08");
			Date start_d = sdformat.parse(start_date);
			Date end_d = sdformat.parse(end_date);

			if (start_d.compareTo(d1) < 0) {
				System.out
						.printf("Usage: start date must be after (or on) 2019-12-31");
				System.exit(-1);
			}

			if (end_d.compareTo(d2) > 0) {
				System.out
						.printf("Usage: end date must be before (or on) 2020-04-08");
				System.exit(-1);
			}

			if (start_d.compareTo(end_d) > 0) {
				System.out.printf("Usage: start date must be before end date");
				System.exit(-1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		/* essential to run any spark code */
		SparkConf conf = new SparkConf().setAppName("SparkCovid19_1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* load input data to RDD */
		JavaRDD<String> dataRDD = sc.textFile(args[0]);

		JavaPairRDD<String, Integer> counts = dataRDD.flatMapToPair(
				new PairFlatMapFunction<String, String, Integer>() {
					public Iterable<Tuple2<String, Integer>> call(String value) {

						String line = value.toString();
						String[] ParsedLine = line.split("\n");

						List<Tuple2<String, Integer>> retWords = new ArrayList<Tuple2<String, Integer>>();

						for (String word : ParsedLine) {
							if (!word.contains("location")
							/*
							 * Some condition satisfying it is header
							 */) {

								String[] ParsedWord = line.split(",");
								if (ParsedWord[0].compareTo(start_date) >= 0
										&& ParsedWord[0].compareTo(end_date) <= 0) {
									retWords.add(new Tuple2<String, Integer>(
											ParsedWord[1], Integer
													.parseInt(ParsedWord[3])));
								}
							}
						}
						return retWords;

					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		counts.saveAsTextFile(output);

	}
}
