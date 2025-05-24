import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class KMeansDriver {
    private static final int MAX_ITERATIONS = 10; // 最大迭代次数
    private static final double CONVERGENCE_THRESHOLD = 0.001; // 收敛阈值

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: KMeansDriver <input path> <output path> <centers path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path centersPath = new Path(args[2]);

        int iteration = 0;
        double previousTotalDistance = Double.MAX_VALUE;

        while (iteration < MAX_ITERATIONS) {
            conf.set("centerFilePath", centersPath.toString());

            Job job = Job.getInstance(conf, "KMeans Clustering - Iteration " + iteration);
            job.setJarByClass(KMeansDriver.class);

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(Combiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Job failed at iteration " + iteration);
                System.exit(1);
            }

            // 计算新的聚类中心与旧的聚类中心之间的总距离
            double totalDistance = calculateTotalDistance(fs, centersPath, outputPath);

            // 检查是否收敛
            if (Math.abs(totalDistance - previousTotalDistance) < CONVERGENCE_THRESHOLD) {
                System.out.println("Converged at iteration " + iteration);
                break;
            }

            // 更新聚类中心文件
            updateCentersFile(fs, centersPath, outputPath);

            // 删除当前迭代的输出目录
            fs.delete(outputPath, true);

            previousTotalDistance = totalDistance;
            iteration++;
        }

        System.out.println("K-Means algorithm completed after " + iteration + " iterations.");
    }

    private static double calculateTotalDistance(FileSystem fs, Path oldCentersPath, Path newCentersPath) throws IOException {
        double totalDistance = 0;
        try (BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldCentersPath)));
             BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(new Path(newCentersPath, "part-r-00000"))))) {
            String oldLine;
            String newLine;
            while ((oldLine = oldReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
                String[] oldValues = oldLine.split(",");
                String[] newValues = newLine.split(",");
                double[] oldCoordinates = new double[oldValues.length];
                double[] newCoordinates = new double[newValues.length];
                for (int i = 0; i < oldValues.length; i++) {
                    oldCoordinates[i] = Double.parseDouble(oldValues[i]);
                }
                for (int i = 0; i < newValues.length; i++) {
                    newCoordinates[i] = Double.parseDouble(newValues[i]);
                }
                Point oldCenter = new Point(oldCoordinates, 0);
                Point newCenter = new Point(newCoordinates, 0);
                totalDistance += oldCenter.distanceTo(newCenter);
            }
        }
        return totalDistance;
    }

    private static void updateCentersFile(FileSystem fs, Path centersPath, Path outputPath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath, "part-r-00000"))));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(centersPath, true)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    // 这里复用KMeansMapper中的Point类
    static class Point {
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
}