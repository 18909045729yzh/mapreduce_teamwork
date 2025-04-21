import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;
public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> 
{	@Override
	protected void map(Object key, Text value, Context context)  
			throws IOException, InterruptedException 
	// default RecordReader: LineRecordReader; key: line offset; value: line string
	{	FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		Text word = new Text();
		Text fileName_lineOffset = new Text(fileName+"#"+key.toString());
		StringTokenizer itr = new StringTokenizer(value.toString());
		for(; itr.hasMoreTokens(); ) 
		{      word.set(itr.nextToken());
		        context.write(word, fileName_lineOffset);
		}
	}
}

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> 
{
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
        Iterator<Text> it = values.iterator();
		StringBuilder all = new StringBuilder();
		if(it.hasNext())  all.append(it.next().toString());
		for(; it.hasNext(); ) {
            all.append(";");
			all.append(it.next().toString());
        }
		context.write(key, new Text(all.toString()));
    }
}

public class InvertedIndexer
{
    public static void main(String[] args) 
    {
        try {
            Configuration conf = new Configuration();
		    job = new Job(conf, "invert index");
		    job.setJarByClass(InvertedIndexer.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setMapperClass(InvertedIndexMapper.class);
		    job.setReducerClass(InvertedIndexReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	       
        } catch (Exception e) {
            e.printStackTrace();
        }
      }
}	