package it.unipi.cloud.hadoop;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class ComputeDistanceMapper extends Mapper<Object, Text, IntWritable, PointWritable> {
    // ..., ..., centroid index, point

    private final PointWritable[] centroids;

    public ComputeDistanceMapper(PointWritable[] centroids) {
        this.centroids = centroids;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        double[] attributes = Arrays
                .stream(value.toString().split(","))
                .mapToDouble(Double::valueOf)
                .toArray();
        PointWritable point = new PointWritable(attributes);

        // For every centroid compute the distance
        // and return the index of the closest centroid
        int bestCentroid = -1;
        double minDistance = Double.POSITIVE_INFINITY;

        for (int i = 0; i < centroids.length; i++) {
            double distance = point.distanceFrom(centroids[i]);

            if (distance < minDistance) {
                minDistance = distance;
                bestCentroid = i;
            }
        }

        if (bestCentroid == -1)
            throw new IOException("There has been an error in assigning the point to a cluster");

        context.write(new IntWritable(bestCentroid), point);
    }
}
