import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private DecimalFormat df = new DecimalFormat("#.00");
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> docFrequency = new HashMap<>();
        int totalCount = 0;
        for(Text value:values){
            String doc = value.toString();
            docFrequency.put(doc, docFrequency.getOrDefault(doc, 0) + 1);
            totalCount++;
        }

        StringBuilder docFreqStr = new StringBuilder();
        for(Map.Entry<String, Integer> entry:docFrequency.entrySet()){
            if(docFreqStr.length() > 0){
                docFreqStr.append(";");
            }
            docFreqStr.append(entry.getKey()).append(":").append(entry.getValue());
        }
        double average = (double) totalCount / docFrequency.size();
        String outputValue = df.format(average) + ", " + docFreqStr.toString();

        
        context.write(key, new Text(outputValue));
    }
}