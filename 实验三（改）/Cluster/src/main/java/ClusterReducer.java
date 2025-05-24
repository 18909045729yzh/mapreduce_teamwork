import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ClusterReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos;

    @Override
    protected void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            mos.write("cluster", NullWritable.get(), val, "cluster-" + key.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
