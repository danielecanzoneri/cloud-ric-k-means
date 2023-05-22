package it.unipi.cloud;

import it.unipi.cloud.model.PointWritable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class Util {

    private static final double EPS = 1e-4;
    public static PointWritable sum(PointWritable p1, PointWritable p2) {

        double[] attr1 = p1.getAttributes();
        double[] attr2 = p2.getAttributes();

        if (attr1.length != attr2.length)
            throw new UnsupportedOperationException("Operands are of different size");

        double[] attrFinal = new double[attr1.length];

        for (int i=0; i < attr1.length; i++)
            attrFinal[i] = attr1[i] + attr2[i];

        PointWritable ret = new PointWritable(attrFinal);
        ret.setCount(p1.getCount() + p2.getCount());

        return ret;
    }

    public static PointWritable computeMean(PointWritable p) {
        double[] attr = p.getAttributes();

        for (int i=0; i < attr.length; i++)
            attr[i] = attr[i] / p.getCount();

        return new PointWritable(attr);
    }

    public static String readCentroids(String centroidsPath) {
        String centroids = "";
        String hdfsPath = "hdfs://10.1.1.45:9820/user/hadoop/" + centroidsPath + "/part-r-00000";
        try {
            FileSystem fs = FileSystem.get(new Configuration());;
            Path filePath = new Path(hdfsPath);

            if (fs.exists(filePath)) {
                // File exists, perform the necessary operations
                // For example, you can open an input stream to read the file
                InputStream inputStream = fs.open(filePath);


                // Read the file content or perform other operations
                String centroidsWithIndex = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                for (String row : centroidsWithIndex.split("\n"))
                    centroids = centroids + row.split("\t")[1] + "\n";

                // Close the input stream when you're done
                inputStream.close();
            } else {
                // File does not exist
                System.out.println("File does not exist: " + hdfsPath);
            }

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return centroids;
    }

    public static boolean stoppingCondition(String _oldCentroids, String _newCentroids) {
        String[] splitOld = _oldCentroids.split("\n");
        String[] splitNew = _newCentroids.split("\n");

        assert splitNew.length == splitOld.length;

        PointWritable[] oldCentroids = new PointWritable[splitOld.length];
        PointWritable[] newCentroids = new PointWritable[splitOld.length];

        for (int i=0; i < splitOld.length; i++) {
            oldCentroids[i] = new PointWritable(splitOld[i]);
            newCentroids[i] = new PointWritable(splitNew[i]);
        }

        for (int i=0; i < splitOld.length; i++) {
            if (oldCentroids[i].distanceFrom(newCentroids[i]) > EPS)
                return false;
        }

        return true;
    }
}
