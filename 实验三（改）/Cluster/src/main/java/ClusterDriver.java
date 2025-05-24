import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ClusterDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ClusterDriver <input> <output> <centers>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("centersPath", args[2]);

        Job job = Job.getInstance(conf, "KMeans Cluster Split");
        job.setJarByClass(ClusterDriver.class);
        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(ClusterReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "cluster", TextOutputFormat.class,
                NullWritable.class, Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
