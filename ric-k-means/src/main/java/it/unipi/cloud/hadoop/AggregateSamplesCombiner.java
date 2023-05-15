package it.unipi.cloud.hadoop;

import it.unipi.cloud.model.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateSamplesCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {
}
