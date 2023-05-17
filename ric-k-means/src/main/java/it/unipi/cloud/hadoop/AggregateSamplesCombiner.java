package it.unipi.cloud.hadoop;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AggregateSamplesCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<PointWritable> iterator = values.iterator();
        PointWritable aggregatePoint = iterator.next();

        while (iterator.hasNext())
            aggregatePoint.sum(iterator.next());

        context.write(key, aggregatePoint);
    }
}
