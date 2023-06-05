package it.unipi.cloud.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class PointWritable implements Writable {

    private double[] attributes;

    // Num of aggregated points combined by Combiner
    private int count;

    public PointWritable() {
        this.attributes = new double[0];
        this.count = 1;
    }

    public PointWritable(String attributes) {
        this.attributes = Arrays
                .stream(attributes.split(","))
                .mapToDouble(Double::valueOf)
                .toArray();

        this.count = 1;
    }

    public double[] getAttributes() {
        return attributes;
    }

    public void setAttributes(double[] attributes) {
        this.attributes = attributes;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(attributes.length);
        for (double attr: attributes)
            out.writeDouble(attr);

        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        int numAttributes = in.readInt();
        attributes = new double[numAttributes];

        for (int i=0; i < numAttributes; i++)
            attributes[i] = in.readDouble();

        count = in.readInt();
    }

    public void sum(PointWritable point) {
        if (point.attributes.length != this.attributes.length)
            throw new UnsupportedOperationException("Points have different number of attributes: " +
                    "this=" + this.attributes.length + ", other=" + point.attributes.length);

        int numAttributes = attributes.length;
        for (int i = 0; i < numAttributes; i++)
            this.attributes[i] += point.attributes[i];

        this.count += point.count;
    }

    public void average() {
        for (int i=0; i < attributes.length; i++)
            attributes[i] /= count;
    }

    public double distanceFrom(PointWritable point) {
        if (point.attributes.length != this.attributes.length)
            throw new UnsupportedOperationException("Points have different number of attributes: " +
                    "this=" + this.attributes.length + ", other=" + point.attributes.length);

        double sum = 0;
        int numAttributes = attributes.length;
        for (int i = 0; i < numAttributes; i++)
            sum += pow(point.attributes[i] - this.attributes[i], 2);

        return sqrt(sum);
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();

        for (double attribute : attributes)
            out.append(attribute).append(",");

        if (out.length() == 0)
            return "";
        return out.substring(0, out.length() - 1);
    }

    public static PointWritable copy(PointWritable point) {
        PointWritable copy = new PointWritable();
        copy.setAttributes(point.getAttributes());
        copy.setCount(point.getCount());

        return copy;
    }
}
