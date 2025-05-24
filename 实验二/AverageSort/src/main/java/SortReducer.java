import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            String[] wordAndDocs = val.toString().split("\t", 2);
            if (wordAndDocs.length != 2) continue;
            String word = wordAndDocs[0];
            String docs = wordAndDocs[1];
            // 拼接 avg 和详细文档频率
            context.write(new Text(word), new Text(String.format("%.2f, %s", key.get(), docs)));
        }
    }
}
