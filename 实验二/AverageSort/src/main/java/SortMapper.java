import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // 原始格式: word \t avg, doc1:cnt1;doc2:cnt2;...
        String[] parts = value.toString().split("\t");
        if (parts.length != 2) return;

        String word = parts[0];
        String[] avgAndDoc = parts[1].split(",", 2); // 只分成 avg 和其余部分
        if (avgAndDoc.length < 2) return;

        try {
            double avg = Double.parseDouble(avgAndDoc[0]);
            String docFreqStr = avgAndDoc[1].trim();
            context.write(new DoubleWritable(avg), new Text(word + "\t" + docFreqStr));
        } catch (NumberFormatException e) {
            // 跳过错误行
        }
    }
}
