package it.unipi.cloud.model;

import org.apache.hadoop.io.Writable;
import sun.jvm.hotspot.types.PointerType;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class PointWritable implements Writable {

    private int numAttributes;
    private double[] attributes;

    // Num of aggregated points combined by Combiner
    private int count;

    public PointWritable() {
        this.numAttributes = 0;
        this.count = 1;
    }

    public PointWritable(double[] attributes) {
        this.numAttributes = attributes.length;
        this.attributes = attributes;
        this.count = 1;
    }

    public PointWritable(String attributesString) {
        String[] split = attributesString.split(",");

        numAttributes = split.length;
        attributes = new double[numAttributes];
        count = 1;

        for (int i=0; i < split.length; i++)
            attributes[i] = Double.parseDouble(split[i]);
    }

    public int getNumAttributes() {
        return numAttributes;
    }

    public void setNumAttributes(int numAttributes) {
        this.numAttributes = numAttributes;
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

    public void sum(PointWritable point) {
        if (point.numAttributes != this.numAttributes)
            throw new UnsupportedOperationException("Points have different number of attributes: this=" + numAttributes + " other:" + point.numAttributes);

        for (int i = 0; i < numAttributes; i++)
            this.attributes[i] += point.attributes[i];

        this.count += point.count;
    }

    public void computeMean() {
        for (int i=0; i < numAttributes; i++)
            attributes[i] /= count;
    }

    public double distanceFrom(PointWritable point) {
        if (point.numAttributes != this.numAttributes)
            throw new UnsupportedOperationException("Points have different number of attributes: this=" + numAttributes + " other:" + point.numAttributes);

        double sum = 0;
        for (int i = 0; i < numAttributes; i++)
            sum += pow(point.attributes[i] - this.attributes[i], 2);

        return sqrt(sum);
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();

        for (double attribute : attributes)
            out.append(attribute).append(",");

        return out.substring(0, out.length() - 1);
    }
}
