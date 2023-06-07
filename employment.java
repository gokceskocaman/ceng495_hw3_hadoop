import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class employment {

    public static class EmploymentMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text actorName = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Skip the header line
            if (((LongWritable) key).get() == 0 && value.toString().contains("title")) {
                return;
            }

            // Split the line by tabs
            String[] fields = value.toString().split("\t");
            if (fields.length >= 10) {
                String actorsString = fields[9];
                String[] actors = actorsString.split(",");
                for (String actor : actors) {
                    if (actor.trim().equals("star")) {
                        continue;
                    }
                    actorName.set(actor.trim());
                    context.write(actorName, one);
                }
            }
        }
    }

    public static class EmploymentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int employmentCount = 0;
            for (IntWritable value : values) {
                employmentCount += value.get();
            }
            context.write(key, new IntWritable(employmentCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "employment");
        job.setJarByClass(employment.class);
        job.setMapperClass(EmploymentMapper.class);
        job.setCombinerClass(EmploymentReducer.class);
        job.setReducerClass(EmploymentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
