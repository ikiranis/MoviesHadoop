/**
 * Created by Yiannis Kiranis <yiannis.kiranis@gmail.com>
 * https://apps4net.eu
 * Date: 5/3/23
 * Time: 7:12 μ.μ.
 *
 * Υπολογισμός των λέξεων που υπάρχουν σε τίτλους των ταινιών
 *
 */

package eu.apps4net;

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

import java.io.IOException;
import java.util.ArrayList;

public class WordsInMovies {
    private static int minimumWordAppearances = 100; // Αριθμός εμφανίσεων της λέξης για να εμφανιστεί στο αποτέλεσμα

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

    public static class MoviesMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Movie movie = null;

            String line = value.toString();

            // Αν η γραμμή είναι η επικεφαλίδα του αρχείου, τότε την παραλείπουμε
            if (line.startsWith("imdbID,")) {
                return;
            }

            // Σπάει τη γραμμή σε στοιχεία
            String[] movieArray = processLine(line);

            // Αν η γραμμή δεν έχει τον αριθμό των στοιχείων που πρέπει, τότε την παραλείπουμε
            if(movieArray == null) {
                return;
            }

            movie = new Movie(movieArray);

            // Προσθήκη των λέξεων στο context του mapper
            StringTokenizer itr = new StringTokenizer(movie.getTitle());
            while (itr.hasMoreTokens()) {
                // Reads each word and removes (strips) the white space
                String token = itr.nextToken().strip();

                // Αν η λέξη είναι μικρότερη από 4 χαρακτήρες, τότε την παραλείπουμε
                if(token.length() < 4) {
                    continue;
                }

                word.set(String.valueOf(token));

                try {
                    one.set(1);

                    context.write(word, one);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static class MoviesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            if(sum > minimumWordAppearances) {
                result.set(sum);

                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movies in year");
        job.setJarByClass(WordsInMovies.class);
        job.setMapperClass(MoviesMapper.class);
        job.setReducerClass(MoviesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(args[2] != null) {
            minimumWordAppearances = Integer.parseInt(args[2]);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Αντικείμενο για την αποθήκευση των δεδομένων της ταινίας
     */
    public static class Movie {
        private final int id;
        private final String title;
        private final String year;
        private final String runtime;
        private final ArrayList<Genre> genres;
        private final String released;
        private final String imdbRating;
        private final String imdbVotes;
        private final String country;

        public Movie(String[] movieArray) {
            this.id = Integer.parseInt(movieArray[0]);
            this.title = movieArray[1];
            this.year = movieArray[2];
            this.runtime = movieArray[3];
            this.genres = new ArrayList<>();

            // Αφαίρεση των εισαγωγικών από το string
            movieArray[4] = movieArray[4].replaceAll("\"", "");

            // Σπάει το string σε πίνακα με βάση τον χαρακτήρα ","
            String[] genresArray = movieArray[4].split(",");

            // Προσθήκη των genres στη λίστα, αφαιρώντας τα πιθανά κενά
            for (String genre : genresArray) {
                // Αν το string είναι κενό, τότε παραλείπεται
                if(genre.equals("")) {
                    continue;
                }

                this.genres.add(new Genre(genre.strip()));
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

        /**
         * Καθαρίζει τον τίτλο της ταινίας από χαρακτήρες που δεν είναι γράμματα ή αριθμοί
         * και τον μετατρέπει σε lowercase
         *
         * @return String
         */
        public String getTitle() {
            return title.replaceAll("[^\\p{L}\\p{Nd}\\s]", "")
                    .replaceAll("\\p{C}", "")
                    .replaceAll("\\s+", " ")
                    .toLowerCase();
        }

        public String getYear() {
            return year;
        }

        public ArrayList<Genre> getGenres() {
            return genres;
        }
    }

    /**
     * Κλάση για τα είδη ταινιών
     */
    public static class Genre {
        private final String name;

        public Genre(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Genre{" +
                    "name='" + name + '\'' +
                    '}';
        }

        public String getName() {
            return name;
        }
    }
}
