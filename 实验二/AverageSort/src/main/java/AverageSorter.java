import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSorter {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageSorter <input-path> <output-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average-sort");

        job.setJarByClass(AverageSorter.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setSortComparatorClass(DescendingDoubleComparator.class); // 降序排序

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));    // InvertedIndexer 的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
