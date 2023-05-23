package it.unipi.cloud.mapreduce;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AggregateSamplesCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<PointWritable> iterator = values.iterator();

        // This is done in order to avoid some weird memory optimization of Hadoop
        // that prevents correct computations
        PointWritable aggregate = PointWritable.copy(iterator.next());

        while (iterator.hasNext())
            aggregate.sum(iterator.next());

        context.write(key, aggregate);
    }
}
