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

public class total {

  public static class TotalMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);
    private Text movie = new Text("total");

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      // Skip the header line
      if (key.get() == 0 && value.toString().contains("name")) {
        return;
      }

      String[] fields = value.toString().split("\t",-1);

      if (fields.length >= 4) {
        String runtimeString = fields[3];
        try {
          long runtime = Long.parseLong(runtimeString);
          context.write(movie, new LongWritable(runtime));
        } catch (NumberFormatException e) {
          // Ignore invalid runtime values
        }
      }
    }
  }

  public static class TotalReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
      long totalTime = 0;
      for (LongWritable value : values) {
        totalTime += value.get();
      }
      context.write(key, new LongWritable(totalTime));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "total");
    job.setJarByClass(total.class);
    job.setMapperClass(TotalMapper.class);
    job.setReducerClass(TotalReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
