package it.unipi.cloud;

import it.unipi.cloud.mapreduce.AggregateSamplesCombiner;
import it.unipi.cloud.mapreduce.ComputeCentroidsReducer;
import it.unipi.cloud.mapreduce.ComputeDistanceMapper;
import it.unipi.cloud.model.PointWritable;
import it.unipi.cloud.util.Util;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
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
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class KMeans {

    private static final int maxIter = 300;
    private static final Configuration conf = new Configuration();
    private static String logPath;

    public static void main(String[] args) throws Exception {

        // Set command line arguments
        Options options = new Options();

        Option clustersOption = new Option("k", "clusters", true, "number of clusters to find");
        clustersOption.setRequired(true); options.addOption(clustersOption);
        Option inputOption = new Option("i", "input", true, "input file containing dataset to cluster");
        inputOption.setRequired(true); options.addOption(inputOption);
        Option outputOption = new Option("o", "output", true, "output folder");
        outputOption.setRequired(true); options.addOption(outputOption);
        Option reducerOption = new Option("r", "reducers", true, "number of reducers [default 1]");
        reducerOption.setRequired(false); options.addOption(reducerOption);
        Option centroidsOption = new Option("C", "centroids", true, "initial centroids [default random]");
        centroidsOption.setRequired(false); options.addOption(centroidsOption);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        try {
            cmd = parser.parse(options, otherArgs);
        } catch (ParseException e) {
            System.out.println(Arrays.toString(args));
            System.out.println(e.getMessage());
            formatter.printHelp("KMeans", options);
            System.exit(1);
        }

        // set the number of clusters to find
        int numClusters = Integer.parseInt(cmd.getOptionValue("clusters"));
        System.out.println("<number_of_clusters> = " + numClusters);
        // input dataset
        String datasetPath = cmd.getOptionValue("input");
        System.out.println("<input> = " + datasetPath);
        // output folder
        String outputPath = cmd.getOptionValue("output");
        System.out.println("<output> = " + outputPath);
        // number of reducers
        int numReducers = 1;
        if (cmd.hasOption("reducers")) {
            numReducers = Integer.parseInt(cmd.getOptionValue("reducers"));
            System.out.println("<number_of_reducer> = " + numReducers);
            conf.set("mapred.reduce.tasks", String.valueOf(numReducers));
        }
        String centroidsPath = null;
        if (cmd.hasOption("centroids")) {
            centroidsPath = cmd.getOptionValue("centroids");
            System.out.println("<input_centroids> = " + centroidsPath);
        }


        // Record time to measure algorithm performances
        long startTime = System.nanoTime();

        // Create log file
        try (FileSystem fs = FileSystem.get(conf)) {
            logPath = outputPath + "/kmeans.log";
            fs.create(new Path(logPath), true);
        }

        String oldCentroids, newCentroids;
        if (centroidsPath == null)
            newCentroids = Util.chooseCentroids(datasetPath, numClusters);
        else {
            FileSystem fs = FileSystem.get(conf);
            InputStream inputStream = fs.open(new Path(Util.hadoopBasePath + centroidsPath));
            newCentroids = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            inputStream.close(); fs.close();
        }
        printLog(newCentroids, 0);

        int iter = 0;
        do {
            iter++;
            System.out.println("\n-----------------------------------------------\n" +
                                "               Iteration n˚ " + iter + "\n" +
                                "-----------------------------------------------\n");
            String tempPath = outputPath + "/temp" + iter;
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
            FileOutputFormat.setOutputPath(job, new Path(tempPath));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            // Keep old centroids in memory to check the stopping condition
            oldCentroids = newCentroids;
            newCentroids = Util.readCentroids(tempPath, numReducers, numClusters);

            // Check if the k-means iteration generated empty clusters
            int numNewCentroids = newCentroids.split("\n").length;
            if (numNewCentroids < numClusters) {
                int emptyClusters = numClusters - numNewCentroids;
                System.out.println(emptyClusters + " cluster" + (emptyClusters > 1 ? "s are" : " is") + " empty, " +
                        "generating new random centroids.");

                // Pick random centroids so the number of centroids is as desired
                newCentroids += Util.chooseCentroids(datasetPath, emptyClusters);
            }

            printLog(newCentroids, iter);

        } while (!Util.stoppingCondition(oldCentroids, newCentroids) && iter < maxIter);

        double timeInSeconds = (System.nanoTime() - startTime) / 1000000000.0;

        printStatistics(newCentroids, iter, timeInSeconds, outputPath);
    }

    private static void printLog(String centroids, int iter) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.append(new Path(logPath));
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

        br.write("-----------------------------------------------\n" +
                "               Iteration n˚ " + iter + "\n" +
                "-----------------------------------------------\n");
        br.write("Centroids: ");
        br.newLine();
        br.write(centroids);
        br.newLine();

        br.close();
        fs.close();
    }

    private static void printStatistics(String centroids, int numIterations, double time, String output) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(output + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

        br.write("Centroids: ");
        br.newLine();
        br.write(centroids);
        br.newLine();

        br.write("Number of iterations: ");
        br.write(String.valueOf(numIterations));
        br.newLine();
        br.write("Average time for iteration: ");
        br.write(String.valueOf(Math.round(time / numIterations * 1000) / 1000.0));
        br.newLine();
        if (numIterations == maxIter) {
            br.newLine();
            br.write("!!! KMeans did not converge !!!");
            br.newLine();
            br.write("Maximum number of iterations reached: " + numIterations);
            br.newLine();
        }

        br.close();
        fs.close();
    }
}
