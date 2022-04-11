package cmsc433.p5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cmsc433.p5.TweetPopularityMR.PopularityReducer;

/**
 * Map reduce which sorts the output of {@link TweetPopularityMR}.
 * The input will either be in the form of: </br>
 * 
 * <code></br>
 * &nbsp;(screen_name,  score)</br>
 * &nbsp;(hashtag, score)</br>
 * &nbsp;(tweet_id, score)</br></br>
 * </code>
 * 
 * The output will be in the same form, but with results sorted on the score.
 * 
 */
public class TweetSortMR {

	/**
	 * Minimum <code>int</code> value for a pair to be included in the output.
	 * Pairs with an <code>int</code> less than this value are omitted.
	 */
	private static int CUTOFF = 10;

	public static class SwapMapper
	extends Mapper<Object, Text, IntWritable, Text> {

		String      id;
		int         score;

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Start swapMapper");
			
			String[] columns = value.toString().split("\t");
			id = columns[0];
			score = Integer.valueOf(columns[1]);

			// TODO: Your code goes here
			context.write(new IntWritable(-1 * score), new Text(id));

		}
	}

	public static class SwapReducer
	extends Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// TODO: Your code goes here
			Iterator<Text> ids = values.iterator();
			
			while(ids.hasNext()) 
			{
				if(-1 * key.get() >= CUTOFF)
				{
					context.write(ids.next(),new IntWritable(-1 * key.get()));
				}else
				{
					ids.next();
				}
			}
			




		}
	}

	/**
	 * This method performs value-based sorting on the given input by configuring
	 * the job as appropriate and using Hadoop.
	 * 
	 * @param job
	 *          Job created for this function
	 * @param input
	 *          String representing location of input directory
	 * @param output
	 *          String representing location of output directory
	 * @return True if successful, false otherwise
	 * @throws Exception
	 */
	public static boolean sort(Job job, String input, String output, int cutoff)
			throws Exception {

		CUTOFF = cutoff;

		job.setJarByClass(TweetSortMR.class);

		// TODO: Set up map-reduce...
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(SwapMapper.class);
		job.setReducerClass(SwapReducer.class);

		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}

}
