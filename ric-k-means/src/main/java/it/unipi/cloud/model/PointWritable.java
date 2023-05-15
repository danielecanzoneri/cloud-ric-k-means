package it.unipi.cloud.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointWritable implements Writable {

    int numAttributes;
    double[] attributes;

    // Num of aggregated points combined by Combiner
    int count;

    public void write(DataOutput out) throws IOException {
        out.writeInt(numAttributes);
        for (double attr: attributes)
            out.writeDouble(attr);

        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        numAttributes = in.readInt();
        attributes = new double[numAttributes];
        for (int i=0; i < numAttributes; i++)
            attributes[i] = in.readDouble();

        count = in.readInt();
    }
}
