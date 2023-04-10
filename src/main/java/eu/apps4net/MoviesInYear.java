/**
 * Created by Yiannis Kiranis <yiannis.kiranis@gmail.com>
 * https://apps4net.eu
 * Date: 5/3/23
 * Time: 7:12 μ.μ.
 *
 * Υπολογισμός των ταινιών που γυρίστηκαν ανα έτος
 *
 */

package eu.apps4net;

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

public class MoviesInYear {

    /**
     * Βρίσκει τη λέξη στον τίτλο
     *
     * @param title
     * @return int
     */
    private static int getYearFromTitle(String title) {
        // Αν η λέξη που βρίσκει δεν είναι αριθμός,
        // σημαίνει ότι στον τίτλο δεν υπάρχει έτος
        try {
            // Παίρνει το substring που βρίσκεται στο τέλος του τίτλου (όπου βρίσκεται το έτος)
            return Integer.parseInt(title.substring(title.length() - 5, title.length() - 1));
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Διάβασμα μιας γραμμής του CSV και σπάσιμο των δεδομένων που υπάρχουν σ' αυτήν
     * Τα δεδομένα περνάνε σε αντικείμενο της κλάσης Movie
     * ,
     * @param line
     * @return Movie
     */
    private static Movie getMovie(String line) {
        // Παίρνει σε array τα κομμάτια της γραμμής που χωρίζονται με ","
        String[] lineFields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        // Διάβασμα του τίτλου και αφαίρεση των εισαγωγικών που υπάρχουν σε κάποιους
        // από αυτούς
        String title = lineFields[1].replace("\"", "");

        // Διαβάζει το έτος
        int year = getYearFromTitle(title);

        // Αφαιρεί το έτος από τον τίτλο, για να μείνει σκέτος αυτός
        title = title.replace(" (" + String.valueOf(year) + ")", "");

        // Δημιουργία του αντικειμένου movie
        Movie movie = new Movie(Long.parseLong(lineFields[0]), title, String.valueOf(year));

        // Διάβασμα των κατηγοριών της ταινίας και προσθήκη αυτών
        // στο αντικείμενο movie
        for (String genre : lineFields[2].split("[|]")) {
            movie.addGenre(genre);
        }

        return movie;
    }

    public static class MoviesMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Movie movie = null;

            String line = value.toString();

            // Αν η γραμμή είναι η επικεφαλίδα του αρχείου, τότε την παραλείπουμε
            if (line.startsWith("movieId,")) {
                return;
            }

            movie = getMovie(line);

            // Προσθήκη του έτους στο context του mapper
            word.set(movie.getYear());

            try {
                one.set(1);

                context.write(word, one);
            } catch (Exception e) {
                System.out.println(e.getMessage());
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

            result.set(sum);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movies in year");
        job.setJarByClass(MoviesInYear.class);
        job.setMapperClass(MoviesMapper.class);
        job.setReducerClass(MoviesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Αντικείμενο για την αποθήκευση των δεδομένων της ταινίας
     */
    public static class Movie {
        private final long movieId;
        private final String title;
        private final String year;
        private final ArrayList<String> genres = new ArrayList<>();

        public Movie(long movieId, String title, String year) {
            this.movieId = movieId;
            this.title = title;
            this.year = year;
        }

        public String getTitle() {
            return title;
        }

        public ArrayList<String> getGenres() {
            return genres;
        }

        public void addGenre(String genre) {
            genres.add(genre);
        }

        public String getYear() {
            return year;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "movieId=" + movieId +
                    ", title='" + title + '\'' +
                    ", year='" + year + '\'' +
                    ", genres=" + genres +
                    '}';
        }
    }
}
