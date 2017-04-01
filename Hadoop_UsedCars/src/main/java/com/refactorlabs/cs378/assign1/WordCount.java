package com.refactorlabs.cs378.assign1;

import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCount {

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	public final static LongWritable ONE = new LongWritable(1L);


	// Puts event_type, page, city, vehicle_condition, year, make, model, trim,
	// body_style, cab_style, carfax_free_report, features
	public static HashMap<String, String> parseUserSession(String userSession){
		HashMap<String, String> result = new HashMap<>();

		String[] allTypes = userSession.split("\t");
		// idx 1 = event_type
		result.put("event_type", allTypes[1]);

		// idx 2 = page
		result.put("page", allTypes[2]);

		// idx 5 = city
		result.put("city", allTypes[5]);

		// idx 7 = vehicle_condition
		result.put("vehicle_condition", allTypes[7]);

		// idx 8 = year
		result.put("year", allTypes[8]);

		// idx 9 = make
		result.put("make", allTypes[9]);

		// idx 10 = model
		result.put("model", allTypes[10]);

		// idx 11 = trim
		result.put("trim", allTypes[11]);

		// idx 12 = body_style
		result.put("body_style", allTypes[12]);

		// idx 13 = cab_style
		result.put("cab_style", allTypes[13]);

		// idx 15 = carfax_free_report
		result.put("carfax_free_report", allTypes[17]);

		result.put("features", allTypes[18]);


		return result;
	}

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			HashMap<String, String> parsedUserSession = parseUserSession(line);


			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			Set<Map.Entry<String, String>> entrySet = parsedUserSession.entrySet();
			for(Map.Entry<String, String> entry : entrySet){
				word.set(entry.getKey() + ":" + entry.getValue());
				context.write(word, ONE);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}


			/*StringTokenizer tokenizer = new StringTokenizer(line);

			// For each word in the input line, emit a count of 1 for that word.
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, ONE);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}*/
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (LongWritable value : values) {
				sum += value.get();
			}
			// Emit the total count for the word.
			context.write(key, new LongWritable(sum));
		}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "WordCount");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordCount.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);


		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}
}

/*
com.refactorlabs.cs378.assign1.WordCount
s3://assignmenttwobucket/data/dataSet5.tsv
s3://assignmenttwobucket/output/Assign5Run1
 */


// Need enums for event_type, event_subtype, body_style, cab_style and vehicle_condition
