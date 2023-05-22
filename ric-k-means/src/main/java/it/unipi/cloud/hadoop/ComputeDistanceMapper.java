package it.unipi.cloud.hadoop;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComputeDistanceMapper extends Mapper<LongWritable, Text, LongWritable, PointWritable> {
    // ..., ..., centroid index, point

    private PointWritable[] centroids;

    @Override
    protected void setup(Context context) {
        String centroidsRaw = context.getConfiguration().get("k-means.centroids");
        String[] centroidsString = centroidsRaw.split("\n");

        centroids = new PointWritable[centroidsString.length];
        for (int i=0; i < centroidsString.length; i++)
            centroids[i] = new PointWritable(centroidsString[i]);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* double[] attributes = Arrays
                .stream(value.toString().split(","))
                .mapToDouble(Double::valueOf)
                .toArray();
         */

        PointWritable point = new PointWritable(value.toString());

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

        context.write(new LongWritable(bestCentroid), point);
    }
}
