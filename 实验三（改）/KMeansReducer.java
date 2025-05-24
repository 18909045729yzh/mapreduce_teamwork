import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Point newCenter = null;
        int totalCount = 0;
        for (Text value : values) {
            String line = value.toString();
            String[] parts = line.split(",");
            int count;
            double[] coordinates;
            if (parts.length > 1) {
                count = Integer.parseInt(parts[parts.length - 1]);
                coordinates = new double[parts.length - 1];
                for (int i = 0; i < parts.length - 1; i++) {
                    coordinates[i] = Double.parseDouble(parts[i]);
                }
            } else {
                continue;
            }
            if (newCenter == null) {
                newCenter = new Point(coordinates, Integer.parseInt(key.toString()));
                totalCount = count;
            } else {
                for (int i = 0; i < coordinates.length; i++) {
                    newCenter.getCoordinates()[i] += coordinates[i] * count;
                }
                totalCount += count;
            }
        }
        if (newCenter != null) {
            newCenter.average(totalCount);
            String coordStr = newCenter.coordinatesToString();
            context.write(key, new Text(coordStr));
        }
    }
}

