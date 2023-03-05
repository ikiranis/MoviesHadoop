package eu.apps4net;

import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * This method uses a regular expression to split each line to a list of strings,
     * each one representing one column
     *
     * source: 2ο θέμα, 3ης εργασία ΠΛΗ47, του 2021-2022
     *
     * @param line string to be split
     */
    private static String[] processLine(String line) {
        // Create a regular expression for proper split of each line

        // The regex for characters other than quote (")
        String otherThanQuote = " [^\"] ";

        // The regex for a quoted string. e.g "whatever1 whatever2"
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);

        // The regex to split the line using comma (,) but taking into consideration the quoted strings
        // This means that is a comma is in a quoted string, it should be ignored.
        String regex = String.format("(?x) " + // enable comments, ignore white spaces
                        ",                         " + // match a comma
                        "(?=                       " + // start positive look ahead
                        "  (?:                     " + //   start non-capturing group 1
                        "    %s*                   " + //     match 'otherThanQuote' zero or more times
                        "    %s                    " + //     match 'quotedString'
                        "  )*                      " + //   end group 1 and repeat it zero or more times
                        "  %s*                     " + //   match 'otherThanQuote'
                        "  $                       " + // match the end of the string
                        ")                         ", // stop positive look ahead
                otherThanQuote, quotedString, otherThanQuote);
        String[] tokens = line.split(regex, -1);

        // check for the proper number of columns
        if (tokens.length == 9) {
            return tokens;
        } else {
            System.err.println("Wrong number of columns for line: " + line);
            return null;
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable movieId = new LongWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Movie movie = null;

            String line = value.toString();

            // Ignore the first line
            if (line.startsWith("imdbID,")) {
                return;
            }

            String movieTitle = "";

            System.out.println(line);
            // Σπάει τη γραμμή σε στοιχεία
            String[] movieArray = processLine(line);

            if(movieArray != null) {
                // Δημιουργία αντικειμένου Tweet
                movie = new Movie(movieArray);

                System.out.println(movie);
                // Παίρνει καθαρό κείμενο από το Tweet
                movieTitle = movie.getTitle();
            }

            StringTokenizer itr = new StringTokenizer(movieTitle);
            while (itr.hasMoreTokens()) {
                // Reads each word and removes (strips) the white space
                String token = itr.nextToken().strip();

//                System.out.println(token);
                word.set(String.valueOf(token));

                try {
                    movieId.set(1);

                    context.write(word, movieId);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static class MoviesReducer extends Reducer<Text, LongWritable, Text, Text> {
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
        Job job = Job.getInstance(conf, "Movies");
        job.setJarByClass(Movies.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(MoviesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Movie {
        private int id;
        private String title;
        private String year;
        private String runtime;
        private ArrayList<Genre> genres;
        private String released;
        private String imdbRating;
        private String imdbVotes;
        private String country;

        public Movie(String[] movieArray) {
            this.id = Integer.parseInt(movieArray[0]);
            this.title = movieArray[1];
            this.year = movieArray[2];
            this.runtime = movieArray[3];
            this.genres = new ArrayList<>();

            // Παίρνει τα είδη ταινίας
            String[] genresArray = movieArray[4].split(",");

            for (String genre : genresArray) {
                this.genres.add(new Genre(genre));
            }

            this.released = movieArray[5];
            this.imdbRating = movieArray[6];
            this.imdbVotes = movieArray[7];
            this.country = movieArray[8];
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "id=" + id +
                    ", title='" + title + '\'' +
                    ", year='" + year + '\'' +
                    ", runtime='" + runtime + '\'' +
                    ", genres=" + genres +
                    ", released='" + released + '\'' +
                    ", imdbRating='" + imdbRating + '\'' +
                    ", imdbVotes='" + imdbVotes + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }

        public String getTitle() {
            return title;
        }
    }

    public static class Genre {
        private String name;

        public Genre(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Genre{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }
}
