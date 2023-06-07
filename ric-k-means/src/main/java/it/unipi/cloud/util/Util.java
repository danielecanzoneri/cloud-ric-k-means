package it.unipi.cloud.util;

import it.unipi.cloud.model.PointWritable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Util {

    private static final double EPS = 1e-4;
    private static final Random random = new Random();
    public static final String hadoopBasePath = "hdfs://10.1.1.45:9820/user/hadoop/";

    private static int datasetSize = -1;

    public static boolean stoppingCondition(String _oldCentroids, String _newCentroids) {
        String[] splitOld = _oldCentroids.split("\n");
        String[] splitNew = _newCentroids.split("\n");

        if (splitNew.length != splitOld.length)
            throw new UnsupportedOperationException("Cannot compare different number od centroids: " +
                    "old=" + splitOld.length + ", new=" + splitNew.length);

        int numCentroids = splitNew.length;

        PointWritable[] oldCentroids = new PointWritable[numCentroids];
        PointWritable[] newCentroids = new PointWritable[numCentroids];

        for (int i=0; i < numCentroids; i++) {
            oldCentroids[i] = new PointWritable(splitOld[i]);
            newCentroids[i] = new PointWritable(splitNew[i]);
        }

        double errorsSum = 0;
        for (int i=0; i < numCentroids; i++)
            errorsSum += oldCentroids[i].distanceFrom(newCentroids[i]);

        return errorsSum < EPS;
    }

    public static String chooseCentroids(String datasetFile, int numCentroids) throws IOException {
        StringBuilder centroids = new StringBuilder();

        Path datasetPath = new Path(datasetFile);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(datasetPath)) {
            FSDataInputStream inputStream = fs.open(datasetPath);
            BufferedReader reader;

            // Count number of samples in the dataset
            // Avoid computing the size of the dataset multiple times
            if (datasetSize == -1) {
                reader = new BufferedReader(new InputStreamReader(inputStream));
                int lines = 0;
                while (reader.readLine() != null)
                    lines++;

                datasetSize = lines;
                reader.close();
            }

            // Select k random rows indexes
            List<Integer> indexes = new ArrayList<>();
            while (indexes.size() < numCentroids) {
                int randomInd = random.nextInt(datasetSize);

                if (!indexes.contains(randomInd))
                    indexes.add(randomInd);
            }
            Collections.sort(indexes);

            // Select random rows
            inputStream = fs.open(datasetPath);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            int k = 0, index = indexes.get(k);
            String line;

            int currentLineNumber = 0;

            while (k < numCentroids) {
                line = reader.readLine();
                if (currentLineNumber == index) {
                    centroids.append(line).append("\n");

                    k++;
                    if (k < numCentroids)
                        index = indexes.get(k);
                }
                currentLineNumber++;
            }

            inputStream.close();
        } else {
            throw new FileNotFoundException("File does not exist: " + datasetPath);
        }
        fs.close();

        return centroids.toString();
    }

    public static String readCentroids(String centroidsFile, int numReducers, int numCentroids) throws IOException {
        // Store centroids with index
        List<String> centroidsWithIndexes = new ArrayList<>();
        String centroidsPath = hadoopBasePath + centroidsFile;

        FileSystem fs = FileSystem.get(new Configuration());
        Path[] centroidPaths = new Path[numReducers];
        for (int i = 0; i < numReducers; i++)
            centroidPaths[i] = new Path(centroidsPath + "/part-r-0000" + i);

        for (Path filePath : centroidPaths) {
            if (fs.exists(filePath)) {
                InputStream inputStream = fs.open(filePath);

                centroidsWithIndexes.addAll(Arrays.asList(IOUtils.toString(inputStream, StandardCharsets.UTF_8).split("\n")));

                // Close the input stream when you're done
                inputStream.close();
            } else {
                throw new FileNotFoundException("File does not exist: " + filePath);
            }
        }

        // Sort centroids by index
        String[] centroids = new String[numCentroids];  // Using numCentroids in case some clusters are empty and
                                                        // centroidsWithIndexes.size() is actually lower than numCentroids
        for (String centroidWithIndex : centroidsWithIndexes) {
            String[] split = centroidWithIndex.split("\t");
            int ind = Integer.parseInt(split[0]);
            centroids[ind] = split[1];
        }

        // Delete temp folder
        fs.delete(new Path(centroidsPath), true);
        fs.close();

        return String.join("\n", centroids);
    }
}
