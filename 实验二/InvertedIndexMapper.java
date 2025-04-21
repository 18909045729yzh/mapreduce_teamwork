import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    // 定义正则表达式模式，匹配单词，忽略大小写
    private static final Pattern WORD_PATTERN = Pattern.compile("\\w+", Pattern.CASE_INSENSITIVE);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // 获取当前输入文件的信息
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        // 构建文件名和行偏移量的组合
        Text fileNames = new Text(fileName);
        // 将输入的文本转换为字符串
        String line = value.toString();
        // 创建 Matcher 对象，用于查找匹配的单词
        Matcher matcher = WORD_PATTERN.matcher(line);
        while (matcher.find()) {
            // 获取匹配到的单词，并转换为小写
            String matchedWord = matcher.group().toLowerCase();
            word.set(matchedWord);
            // 输出键值对：单词 -> 文件名#行偏移量
            context.write(word, fileNames);
        }
    }
}