package it.unipi.cloud.hadoop;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AggregateSamplesCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<PointWritable> iterator = values.iterator();
        PointWritable copy = iterator.next();

        PointWritable aggregatePoint = new PointWritable();
        aggregatePoint.setAttributes(copy.getAttributes());
        aggregatePoint.setCount(copy.getCount());

        while (iterator.hasNext())
            aggregatePoint.sum(iterator.next());

        context.write(key, aggregatePoint);
    }
}
