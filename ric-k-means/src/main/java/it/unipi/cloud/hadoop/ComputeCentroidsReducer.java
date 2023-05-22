package it.unipi.cloud.hadoop;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ComputeCentroidsReducer extends Reducer<LongWritable, PointWritable, LongWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<PointWritable> iterator = values.iterator();
        PointWritable copy = iterator.next();

        PointWritable centroid = new PointWritable();
        centroid.setAttributes(copy.getAttributes());
        centroid.setCount(copy.getCount());

        while (iterator.hasNext())
            centroid.sum(iterator.next());

        centroid.computeMean();

        context.write(key, new Text(centroid.toString()));
    }
}
