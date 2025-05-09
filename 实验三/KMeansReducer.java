import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Point newCenter = null;
        int totalCount = 0;

        // 遍历相同聚类中心的所有值
        for (Text value : values) {
            String line = value.toString();
            String[] parts = line.split(",");
            int count = Integer.parseInt(parts[parts.length - 1]);
            double[] coordinates = new double[parts.length - 1];

            // 提取坐标值
            for (int i = 0; i < parts.length - 1; i++) {
                coordinates[i] = Double.parseDouble(parts[i]);
            }

            if (newCenter == null) {
                // 初始化新的聚类中心
                newCenter = new Point(coordinates, Integer.parseInt(key.toString()));
                totalCount = count;
            } else {
                // 累加坐标值
                for (int i = 0; i < coordinates.length; i++) {
                    newCenter.getCoordinates()[i] += coordinates[i] * count;
                }
                totalCount += count;
            }
        }

        if (newCenter != null) {
            // 计算平均值得到新的聚类中心
            newCenter.average(totalCount);
            String coordStr = newCenter.coordinatesToString();
            // 输出新的聚类中心
            context.write(key, new Text(coordStr));
        }
    }
}

