import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text clusterID, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        Point pm = null;
        int n=0;
        for(Text value:values) {
            String line = value.toString();
            String[] position = line.split(",|:");
            int Id = Integer.parseInt(position[0]);
            double[] coordinates = new double[position.length - 2];
            for (int i = 1; i < position.length - 1; i++) {
                coordinates[i - 1] = Double.parseDouble(position[i]);
            }
            if (pm == null) {
                pm = new Point(coordinates, Integer.parseInt(clusterID.toString()));
            } else {
                pm.plus(coordinates);
            }
            n++;
        }
        String coordStr = pm.coordinatesToString();
        context.write(clusterID, new Text(coordStr+","+n));
        }
}