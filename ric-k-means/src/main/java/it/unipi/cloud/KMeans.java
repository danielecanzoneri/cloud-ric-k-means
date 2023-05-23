package it.unipi.cloud;

import it.unipi.cloud.mapreduce.AggregateSamplesCombiner;
import it.unipi.cloud.mapreduce.ComputeCentroidsReducer;
import it.unipi.cloud.mapreduce.ComputeDistanceMapper;
import it.unipi.cloud.model.PointWritable;
import it.unipi.cloud.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {

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

        String datasetPath = otherArgs[1];

        // set the number of clusters to find
        int numClusters = Integer.parseInt(otherArgs[0]);

        String oldCentroids, newCentroids;
        newCentroids = Util.chooseCentroids(datasetPath, numClusters);

        int iter = 0;
        do {
            iter++;
            System.out.println("\n-----------------------------------------------\n" +
                                "               Iteration n˚ " + iter + "\n" +
                                "-----------------------------------------------\n");
            String outputPath = otherArgs[2] + "/temp" + iter;
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

            // Set starting centroids for a k-means iteration
            job.getConfiguration().set("k-means.centroids", newCentroids);

            // define I/O
            FileInputFormat.addInputPath(job, new Path(datasetPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            // Keep old centroids in memory to check the stopping condition
            oldCentroids = newCentroids;
            newCentroids = Util.readCentroids(outputPath);

            // Check if the k-means iteration generated empty clusters
            int numNewCentroids = newCentroids.split("\n").length;
            if (numNewCentroids < numClusters) {
                int emptyClusters = numClusters - numNewCentroids;
                System.out.println(emptyClusters + " cluster" + (emptyClusters > 1 ? "s" : "") + " was empty, " +
                        "generating new random centroids.");

                // Pick random centroids so the number of centroids is as desired
                newCentroids += Util.chooseCentroids(datasetPath, emptyClusters);
            }

        } while (!Util.stoppingCondition(oldCentroids, newCentroids));

        FileSystem fs = FileSystem.get(conf);

        FileUtil.copy(fs, new Path(otherArgs[2] + "/temp" + iter + "/part-r-00000"),
                fs, new Path(otherArgs[2] + "/centroids.txt"), false, conf);
    }
}
