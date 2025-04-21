import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexer {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: InvertedIndexer <input-path> <output-path>");
            System.exit(1);
        }

        try {
            // 配置作业
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "invert-index");
            job.setJarByClass(InvertedIndexer.class);

            // 设置Mapper和Reducer
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);

            // 设置输入输出格式和类型
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // 设置输入输出路径
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 提交作业并等待完成
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}