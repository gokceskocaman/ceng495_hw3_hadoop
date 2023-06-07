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

  public static class TotalMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);
    private Text movie = new Text("total");

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

      if (key.get() == 0 && value.toString().contains("title")) {
        return;
      }


      String[] fields = value.toString().split("\t");
      if (fields.length >= 15) {
        String runtimeString = fields[14];
        try {
          double runtime = Double.parseDouble(runtimeString);
          context.write(movie, new DoubleWritable(runtime));
        } catch (NumberFormatException e) {

        }
      }
    }
  }

  public static class TotalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
      double totalTime = 0;
      for (DoubleWritable value : values) {
        totalTime += value.get();
      }
      context.write(key, new DoubleWritable(totalTime));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "total");
    job.setJarByClass(total.class);
    job.setMapperClass(TotalMapper.class);
    job.setReducerClass(TotalReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
