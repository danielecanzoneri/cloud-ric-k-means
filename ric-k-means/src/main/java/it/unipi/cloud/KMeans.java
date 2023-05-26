package it.unipi.cloud;

import it.unipi.cloud.mapreduce.AggregateSamplesCombiner;
import it.unipi.cloud.mapreduce.ComputeCentroidsReducer;
import it.unipi.cloud.mapreduce.ComputeDistanceMapper;
import it.unipi.cloud.model.PointWritable;
import it.unipi.cloud.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class KMeans {

    private static final Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: KMeans <number_of_clusters> <input> <output> [<number_of_reducer> <use_of_combiner>]");
            System.exit(1);
        }
        System.out.println("args[0]: <number_of_clusters>="+otherArgs[0]);
        System.out.println("args[1]: <input>="+otherArgs[1]);
        System.out.println("args[2]: <output>="+otherArgs[2]);
        int numReducer = 1;
        if (otherArgs.length == 4) {
            System.out.println("args[3]: <number_of_reducer>="+otherArgs[3]);
            conf.set("mapred.reduce.tasks", otherArgs[3]);
            numReducer = Integer.parseInt(otherArgs[3]);
        }
        boolean useCombiner = true;
        if (otherArgs.length == 5) {
            System.out.println("args[4]: <use_of_combiner>="+otherArgs[4]);
            useCombiner = Boolean.parseBoolean(otherArgs[4]);
        }

        String datasetPath = otherArgs[1];

        // set the number of clusters to find
        int numClusters = Integer.parseInt(otherArgs[0]);

        // Record time to measure algorithm performances
        long startTime = System.nanoTime();

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
            if (useCombiner)
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
            newCentroids = Util.readCentroids(outputPath, numReducer);

            // Check if the k-means iteration generated empty clusters
            int numNewCentroids = newCentroids.split("\n").length;
            if (numNewCentroids < numClusters) {
                int emptyClusters = numClusters - numNewCentroids;
                System.out.println(emptyClusters + " cluster" + (emptyClusters > 1 ? "s are" : " is") + " empty, " +
                        "generating new random centroids.");

                // Pick random centroids so the number of centroids is as desired
                newCentroids += Util.chooseCentroids(datasetPath, emptyClusters);
            }

        } while (!Util.stoppingCondition(oldCentroids, newCentroids));

        double timeInSeconds = (System.nanoTime() - startTime) / 1000000000.0;

        printStatistics(newCentroids, iter, timeInSeconds, otherArgs[2]);
    }

    private static void printStatistics(String centroids, int numIterations, double time, String output) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(output + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

        br.write("Centroids");
        br.newLine();
        br.write(centroids);
        br.newLine();

        br.write("Number of iterations: ");
        br.write(String.valueOf(numIterations));
        br.newLine();
        br.write("Average time for iteration: ");
        br.write(String.valueOf(Math.round(time / numIterations * 1000) / 1000.0));
        br.newLine();

        br.close();
        fs.close();
    }
}
