package it.unipi.cloud.model;

import org.apache.hadoop.io.Writable;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class PointWritable implements Writable {

    int numAttributes;
    double[] attributes;

    // Num of aggregated points combined by Combiner
    int count;

    public PointWritable(double[] attributes) {
        this.numAttributes = attributes.length;
        this.attributes = attributes;
        this.count = 1;
    }

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

    public double distanceFrom(PointWritable point) {
        double sum = 0;
        for (int i = 0; i < numAttributes; i++)
            sum += pow(point.attributes[i] - this.attributes[i], 2);

        return sqrt(sum);
    }
}
