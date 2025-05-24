import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFMapper extends Mapper<Object, Text, Text, Text> {
    private static final Pattern WORD_PATTERN = Pattern.compile("\\w+", Pattern.CASE_INSENSITIVE);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        Matcher matcher = WORD_PATTERN.matcher(line);
        while (matcher.find()) {
            String word = matcher.group();
            context.write(new Text(word), new Text(fileName));
        }
    }
}
