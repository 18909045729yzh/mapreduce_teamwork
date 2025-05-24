import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class ClusterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private Map<Integer, double[]> centers = new HashMap<>();

    @Override
    protected void setup(Context context) throws java.io.IOException {
        String centersPath = context.getConfiguration().get("centersPath");
        Path path = new Path(centersPath);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            int centerId = Integer.parseInt(parts[0]);
            String[] comps = parts[1].split(",");
            double[] vec = new double[comps.length];
            for (int i = 0; i < comps.length; i++) {
                vec[i] = Double.parseDouble(comps[i]);
            }
            centers.put(centerId, vec);
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        // 处理如 0: 7, 2, 1, 0, ...
        String line = value.toString();
        int colonIndex = line.indexOf(":");
        if (colonIndex < 0) return;

        String id = line.substring(0, colonIndex).trim();
        String[] comps = line.substring(colonIndex + 1).split(",");
        double[] vec = new double[comps.length];
        for (int i = 0; i < comps.length; i++) {
            vec[i] = Double.parseDouble(comps[i].trim());
        }

        // 找最近的聚类中心
        int bestCluster = -1;
        double minDist = Double.MAX_VALUE;
        for (Map.Entry<Integer, double[]> entry : centers.entrySet()) {
            double[] center = entry.getValue();
            double dist = 0;
            for (int i = 0; i < vec.length; i++) {
                dist += Math.pow(vec[i] - center[i], 2);
            }
            if (dist < minDist) {
                minDist = dist;
                bestCluster = entry.getKey();
            }
        }

        context.write(new IntWritable(bestCluster), value);
    }
}
