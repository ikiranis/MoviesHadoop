package eu.apps4net;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Yiannis Kiranis <yiannis.kiranis@gmail.com>
 * https://apps4net.eu
 * Date: 5/3/23
 * Time: 7:12 μ.μ.
 */

public class Movies {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable tweetId = new LongWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Tweet tweet = null;

            String line = value.toString();
            String tweetText = "";

            // Σπάει την γραμμή σε στοιχεία
            String[] tweetArray = processLine(line);

            if(tweetArray != null) {
                // Δημιουργία αντικειμένου Tweet
                tweet = new Tweet(tweetArray);

                // Παίρνει καθαρό κείμενο από το Tweet
                tweetText = tweet.getClearedText();
            }

            StringTokenizer itr = new StringTokenizer(tweetText);
            while (itr.hasMoreTokens()) {
                // Reads each word and removes (strips) the white space
                String token = itr.nextToken().strip();

//                System.out.println(token);
                word.set(String.valueOf(token));

                try {
                    tweetId.set((long) Double.parseDouble(tweet.getTweetId()));

                    context.write(word, tweetId);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static class TweetsReducer extends Reducer<Text, LongWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder text = new StringBuilder();

            for (LongWritable val : values) {
                text.append(String.valueOf(val)).append(" ");
            }

            result.set(String.valueOf(text));

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Airline tweets");
        job.setJarByClass(Movies.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TweetsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
