import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class Apriori {

  private static final List<String> frequentItems = new ArrayList<>();

  public static class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int minSup; // User-defined minimum support threshold

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      minSup = conf.getInt("minSup", 10); // Set minimum support to 10
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Split the transaction record based on your data format (modify as needed)
      String[] fruits = value.toString().split(":")[1].trim().split(",\\s*");
      for (String fruit : fruits) {
        word.set(fruit);
        context.write(word, one);
      }
    }
  }

  public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private boolean frequentItemWritten = false;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      // Minimum support is already set in the Mapper setup (modify if needed)
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result); // Emit <item, frequency>
      
      // Add frequent items to the list for later access
      if (sum >= 10) {
        synchronized (frequentItems) {
          frequentItems.add(key.toString());
        }
        frequentItemWritten = true;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (frequentItemWritten) {
        context.write(new Text("\nThe min support is set to 10, so frequent items above this threshold are:"), null);
        for (String item : frequentItems) {
          context.write(new Text(item),null);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Optional: Set minimum support threshold from arguments (if different from 10)
    // if (args.length > 2) {
    //   conf.setInt("minSup", Integer.parseInt(args[2]));
    // }

    Job job = Job.getInstance(conf, "apriori");
    job.setJarByClass(Apriori.class);
    job.setMapperClass(AprioriMapper.class);
    // Set combiner if needed for subsequent passes (comment out for now)
    // job.setCombinerClass(AprioriReducer.class);
    job.setReducerClass(AprioriReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
