package cmsc433.p5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final int          TWEET_SCORE   = 1;
	public static final int          RETWEET_SCORE = 3;
	public static final int          MENTION_SCORE = 1;
	public static final int			 PAIR_SCORE = 1;

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	public static class TweetMapper
	extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object tp, Text csv, Context context)
				throws IOException, InterruptedException {
			System.out.println("IN Popularity Mapper rn");
			
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(csv.toString());

			// TODO: Your code goes here
			if(trendingOn == TrendingParameter.HASHTAG) 
			{
				//context.write(new Text("no hashtag"), new IntWritable(1));
				
				ArrayList<String> hashtags = (ArrayList<String>) tweet.getHashtags();
				for(String ht :hashtags)
				{
					context.write(new Text(ht.replace("#", "")) , new IntWritable(1));
				}
				
			}
			else if(trendingOn == TrendingParameter.HASHTAG_PAIR)
			{
				//context.write(new Text("no hashtag"), new IntWritable(1));
				
				ArrayList<String> hashtags = (ArrayList<String>) tweet.getHashtags();
				int currIndex = 0;
				
				for(int i = 0; i < hashtags.size(); i++)
				{
					for(int j = currIndex; j < hashtags.size(); j++)
					{
						if(i != j) 
						{
							String str;
							if(hashtags.get(i).toLowerCase().compareTo(hashtags.get(j).toLowerCase()) < 0) {
								str = "(" + hashtags.get(j).replace("#","") + "," + hashtags.get(i).replace("#", "") + ")";
							}else
							{
								str = "(" + hashtags.get(i).replace("#","") + "," + hashtags.get(j).replace("#", "") + ")";
							}
							
							context.write(new Text(str), new IntWritable(1));
						}
					}
					currIndex++;
				}
				
				
			}
			else if(trendingOn == TrendingParameter.TWEET)
			{
				if(tweet.wasRetweetOfTweet())
				{
					context.write(new Text(tweet.getId().toString()), new IntWritable(1));
					context.write(new Text(tweet.getRetweetedTweet().toString()), new IntWritable(3));
				}
				else
				{
					context.write(new Text(tweet.getId().toString()), new IntWritable(1));
				}
			}
			else if(trendingOn == TrendingParameter.USER)
			{
				//1 point for each tweet a user tweets
				context.write(new Text(tweet.getUserScreenName()), new IntWritable(1));
				
				//1 point for each time each user gets mentioned
				ArrayList<String> mentions = (ArrayList<String>) tweet.getMentionedUsers();
				for(String mention :mentions)
				{
					context.write(new Text(mention), new IntWritable(1));
				}
				
				//3 points for each time a user's tweet gets retweeted
				if(tweet.wasRetweetOfUser())
				{
					context.write(new Text(tweet.getRetweetedUser()) , new IntWritable(3));
				}
				
				
			}
			else {
				System.out.println("TrendingOn error!");
			}




		}
	}

	public static class PopularityReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("IN Popularity Reducer rn");
			
			// TODO: Your code goes here
			int sum = 0;
			Iterator<IntWritable> scores = values.iterator();
			while(scores.hasNext()) 
			{
				sum += scores.next().get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;

		job.setJarByClass(TweetPopularityMR.class);

		// TODO: Set up map-reduce...
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);
		
		//end
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		
		return job.waitForCompletion(true);
	}
}
