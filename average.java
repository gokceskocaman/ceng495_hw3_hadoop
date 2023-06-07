import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class average {
    public static class AverageMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] columns = value.toString().split("\t", -1);
        if (columns.length >= 15) {
            String runtime = columns[14];
            try {
            double r = Double.parseDouble(runtime);
            result.set(r);
            context.write(new Text("average"), result);
            } catch (NumberFormatException e) {
            }
        }
        }
    }

    public static class AverageReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }
        result.set(sum / count);
        context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average");
        job.setJarByClass(average.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
