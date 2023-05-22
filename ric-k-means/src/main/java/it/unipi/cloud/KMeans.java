package it.unipi.cloud;


import it.unipi.cloud.hadoop.AggregateSamplesCombiner;
import it.unipi.cloud.hadoop.ComputeCentroidsReducer;
import it.unipi.cloud.hadoop.ComputeDistanceMapper;
import it.unipi.cloud.model.PointWritable;
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

import java.io.File;

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


        // set the number of clusters to find
        int numClusters = Integer.parseInt(otherArgs[0]);

        String oldCentroids, newCentroids;
        newCentroids = Util.chooseCentroids(new Path(otherArgs[1]), numClusters);

        int iter = 0;
        do {
            String outputPath = otherArgs[2] + "/temp";
            iter++;

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

            // Set startup parameters for a k-means iteration
            job.getConfiguration().set("k-means.centroids", newCentroids);

            // define I/O
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            // Keep old centroids in memory to check for completion
            oldCentroids = newCentroids;
            newCentroids = Util.readCentroids(outputPath);

        } while (!Util.stoppingCondition(oldCentroids, newCentroids));

        // Clean tmp directories
        FileSystem fs = FileSystem.get(conf);

        FileUtil.copy(fs, new Path(otherArgs[2] + "/temp" + (iter-1) + "/part-r-00000"),
                fs, new Path(otherArgs[2] + "/centroids.txt"), false, conf);
    }
}
