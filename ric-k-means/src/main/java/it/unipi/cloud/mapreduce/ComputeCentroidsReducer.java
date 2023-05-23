package it.unipi.cloud.mapreduce;

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

        // This is done in order to avoid some weird memory optimization of Hadoop
        // that prevents correct computations
        PointWritable centroid = PointWritable.copy(iterator.next());

        while (iterator.hasNext())
            centroid.sum(iterator.next());

        centroid.average();
        context.write(key, new Text(centroid.toString()));
    }
}
