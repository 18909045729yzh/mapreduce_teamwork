import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: KMeansDriver <input path> <output path> <centers path> <k>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        conf.set("k", args[3]);
        conf.set("centerFilePath", args[2]);
        
        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansDriver.class);

        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}