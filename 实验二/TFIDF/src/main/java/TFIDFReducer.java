import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    int docCount = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        docCount = conf.getInt("docCount", 1); // 默认值为 1
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> tfMap = new HashMap<>();
        Set<String> docSet = new HashSet<>();

        for (Text val : values) {
            String doc = val.toString();
            tfMap.put(doc, tfMap.getOrDefault(doc, 0) + 1);
            docSet.add(doc);
        }

        int docCountContainingWord = docSet.size();
        double idf = Math.log((double) docCount / (docCountContainingWord + 1));

        for (Map.Entry<String, Integer> entry : tfMap.entrySet()) {
            String doc = entry.getKey();
            int tf = entry.getValue();
            double tfidf = tf * idf;

            // 输出格式：文档名\t单词\tTF-IDF
//            context.write(new Text(doc), new Text(key.toString() + "\t" + String.format("%.6f", tfidf)));
            //输出格式：文档名,单词,TF-IDF
            context.write(null, new Text(doc + "," + key.toString() + "," + String.format("%.6f", tfidf)));

        }
    }
}
