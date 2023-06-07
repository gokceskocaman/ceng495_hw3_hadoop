import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class genrescore {


    public static class GenreScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text genre = new Text();
        private DoubleWritable score = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] columns = value.toString().split("\t",-1);


            if(columns.length < 6) {
                return;
            }

            String ratingVal = columns[2].trim();
            String votesVal = columns[5].trim();

            if(ratingVal.equals("rating") || votesVal.equals("numVotes")) {
                return;
            }

            try {
                double v = Double.parseDouble(votesVal);
                genre.set(ratingVal);
                score.set(v);
                context.write(genre, score);
            } catch (NumberFormatException e) {

            }

        }
    }

    public static class GenreScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 9) {
                double avg = sum / count;
                average.set(avg);
                context.write(key, average);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "genrescore");
        job.setJarByClass(genrescore.class);
        job.setMapperClass(GenreScoreMapper.class);
        job.setReducerClass(GenreScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
