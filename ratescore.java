import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ratescore {


    public static class RateScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text rating = new Text();
        private DoubleWritable votes = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] columns = value.toString().split("\t",-1);

            String ratingVal = columns[1].trim();
            String votesVal = columns[6].trim();

            if(ratingVal.equals("rating") || votesVal.equals("numVotes")) {
                return;
            }

            try {
                if (ratingVal.equals("G")) {
                    rating.set("G");
                    double vote = Double.parseDouble(votesVal);
                    votes.set(vote);
                    context.write(rating, votes);
                } else if (ratingVal.equals("PG-13")) {
                    rating.set("PG-13");
                    double vote = Double.parseDouble(votesVal);
                    votes.set(vote);
                    context.write(rating, votes);
                } else if (ratingVal.equals("R")) {
                    rating.set("R");
                    double vote = Double.parseDouble(votesVal);
                    votes.set(vote);
                    context.write(rating, votes);
                } else if (ratingVal.equals("PG")) {
                    rating.set("PG");
                    double vote = Double.parseDouble(votesVal);
                    votes.set(vote);
                    context.write(rating, votes);
                }

            } catch (NumberFormatException e) {
                return;
            }





        }
    }

    public static class RateScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 0) {
                double avg = sum / count;
                average.set(avg);
                context.write(key, average);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ratescore");
        job.setJarByClass(ratescore.class);
        job.setMapperClass(RateScoreMapper.class);
        job.setCombinerClass(RateScoreReducer.class);
        job.setReducerClass(RateScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
