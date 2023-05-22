package it.unipi.cloud.hadoop;

import it.unipi.cloud.Util;
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
        PointWritable centroid = iterator.next();

        while (iterator.hasNext())
            centroid = Util.sum(centroid, iterator.next());

        centroid = Util.computeMean(centroid);

        context.write(key, new Text(centroid.toString()));
    }
}
