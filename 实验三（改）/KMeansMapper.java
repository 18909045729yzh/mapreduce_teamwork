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
import org.apache.hadoop.mapreduce.Mapper;

class Point {
    private int Id;
    private double[] coordinates;

    public Point(double[] coordinates, int Id) {
        this.coordinates = coordinates;
        this.Id = Id;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public int getId() {
        return Id;
    }

    public double distanceTo(Point other) {
        double sum = 0;
        for (int i = 0; i < coordinates.length; i++) {
            double diff = coordinates[i] - other.getCoordinates()[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public void plus(double[] other) {
        for(int i = 0; i < other.length; i++) {
            coordinates[i] += other[i];
        }
    }

    public void average(int n) {
        for(int i = 0; i < coordinates.length; i++) {
            coordinates[i] /= n;
        }
    }

    public String coordinatesToString() {
        StringBuilder sb = new StringBuilder();
        for (double coord : coordinates) {
            sb.append(coord).append(",");
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    private List<Point> centers = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("centerFilePath")))))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",|:");
                double[] coordinates = new double[values.length - 1];
                int Id = Integer.parseInt(values[0]);
                System.out.println("解析行: " + line);
                System.out.println("values 数组长度: " + values.length);
                System.out.println("coordinates 数组长度: " + coordinates.length);
                for (int i = 1; i < values.length; i++) {
                    try {
                        coordinates[i - 1] = Double.parseDouble(values[i]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        System.err.println("索引越界异常: i = " + i + ", values.length = " + values.length + ", coordinates.length = " + coordinates.length);
                        throw e;
                    }
                }

                centers.add(new Point(coordinates, Id));
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split(",|:");
        double[] coordinates = new double[values.length - 1];
        int Id = Integer.parseInt(values[0]);
        for (int i = 1; i < values.length; i++) {
            coordinates[i - 1] = Double.parseDouble(values[i]);
        }
        Point p = new Point(coordinates, Id);

        double minDis = Double.MAX_VALUE;
        int index = -1;
        for (int i = 0; i < centers.size(); i++) {
            Point center = centers.get(i);
            double dis = p.distanceTo(center);
            if (dis < minDis) {
                minDis = dis;
                index = i;
            }
        }

        if (index != -1) {
            context.write(new Text(String.valueOf(centers.get(index).getId())), new Text(line + ",1"));
        } else {
            System.err.println("警告：未找到最近的聚类中心，数据点: " + line);
        }
    }
}