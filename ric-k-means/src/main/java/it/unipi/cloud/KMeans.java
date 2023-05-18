package it.unipi.cloud;


import it.unipi.cloud.hadoop.AggregateSamplesCombiner;
import it.unipi.cloud.hadoop.ComputeCentroidsReducer;
import it.unipi.cloud.hadoop.ComputeDistanceMapper;
import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Random;

public class KMeans {

    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: KMeans <number_of_clusters> <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <number_of_clusters>="+otherArgs[0]);
        System.out.println("args[1]: <input>="+otherArgs[1]);
        System.out.println("args[2]: <output>="+otherArgs[2]);

        Job job = Job.getInstance(conf, "K-Means");
        job.setJarByClass(KMeans.class);

        // set mapper/combiner/reducer
        job.setMapperClass(ComputeDistanceMapper.class);
        job.setCombinerClass(AggregateSamplesCombiner.class);
        job.setReducerClass(ComputeCentroidsReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // set the number of clusters to find
        int numClusters = Integer.parseInt(otherArgs[0]);
        int numAttributes = 50;
        /*
        try (BufferedReader reader = new BufferedReader(new FileReader(otherArgs[1]))) {
            String firstLine = reader.readLine();
            numAttributes = firstLine.split(",").length;
        }
        */
        job.getConfiguration().setInt("kmeans.num_clusters", numClusters);
        job.getConfiguration().set("kmeans.centroids", chooseCentroids(numClusters, numAttributes));

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static String chooseCentroids(int numCentroids, int numAttributes) {
        StringBuilder centroids = new StringBuilder();

        for (int c = 0; c < numCentroids; c++) {
            for (int attr = 0; attr < numAttributes; attr++) {
                centroids.append(random.nextDouble() * 5);
                if (attr < numAttributes-1)
                    centroids.append(",");
            }
            centroids.append("\n");
        }

        return centroids.toString();
    }

    private String chooseCentroids(Path datasetPath, int numCentroids) {
        /*
        List<String> randomRows = new ArrayList<>();
        Random random = new Random();
        String line;
        int totalRows = 0;
        while ((line = reader.readLine()) != null) {
            totalRows++;
            if (randomRows.size() < k) {
                randomRows.add(line);
            } else {
                int randomIndex = random.nextInt(totalRows);
                if (randomIndex < k) {
                    randomRows.set(randomIndex, line);
                }
            }
        }

        return randomRows;
         */
        return null;
    }
}
