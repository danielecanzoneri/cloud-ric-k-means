package it.unipi.cloud;

import it.unipi.cloud.model.PointWritable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Util {

    private static final double EPS = 1e-4;
    private static final Random random = new Random();

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
        String hdfsPath = "hdfs://10.1.1.45:9820/user/hadoop/" + centroidsPath;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            Path filePath = new Path(hdfsPath + "/part-r-00000");

            if (fs.exists(filePath)) {
                InputStream inputStream = fs.open(filePath);

                String centroidsWithIndex = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                for (String row : centroidsWithIndex.split("\n"))
                    centroids = centroids + row.split("\t")[1] + "\n";

                // Close the input stream when you're done
                inputStream.close();
            } else {
                System.out.println("File does not exist: " + hdfsPath);
            }

            // fs.delete(new Path(centroidsPath), true);
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
        int numCentroids = splitNew.length;

        PointWritable[] oldCentroids = new PointWritable[numCentroids];
        PointWritable[] newCentroids = new PointWritable[numCentroids];

        for (int i=0; i < numCentroids; i++) {
            oldCentroids[i] = new PointWritable(splitOld[i]);
            newCentroids[i] = new PointWritable(splitNew[i]);
        }

        for (int i=0; i < numCentroids; i++) {
            if (oldCentroids[i].distanceFrom(newCentroids[i]) > EPS)
                return false;
        }

        return true;
    }


    public static String chooseCentroids(int numCentroids, int numAttributes) {
        return "2.175983436853002,3.4380952380952383,2.9685300207039336,3.590062111801242,2.681159420289855,2.975569358178054,2.1805383022774327,3.756935817805383,2.567287784679089,4.010766045548654,2.6853002070393375,3.6443064182194616,3.3838509316770184,2.991304347826087,2.5598343685300207,2.214492753623188,2.505175983436853,2.084472049689441,2.65879917184265,2.3354037267080745,2.74120082815735,3.322567287784679,2.2782608695652176,3.3734989648033125,2.8472049689440992,3.186335403726708,2.759834368530021,3.25879917184265,3.175983436853002,3.2422360248447206,3.3643892339544514,2.6633540372670805,3.8770186335403727,2.331262939958592,2.64472049689441,2.5527950310559007,3.658385093167702,2.4712215320910973,3.1693581780538302,3.5204968944099377,3.405383022774327,2.24472049689441,3.6149068322981366,2.301449275362319,3.5370600414078677,2.241407867494824,3.8993788819875776,2.920496894409938,3.782194616977226,3.581366459627329\n" +
                "1.791191506095163,3.6350766810853323,2.4007078254030674,4.072748721981911,2.418403460479748,3.1545418796696816,1.7746755800235943,4.164372788045616,2.1329138812426267,4.493904836806921,4.237121510027526,2.533228470310657,4.539520251671254,2.1694848604011012,3.4707038930397167,3.79315768777035,3.872198191112859,3.580023594180102,3.9091624066063706,3.7715296893432955,2.292567833267794,3.524577270939835,2.4125049154541878,3.9787652379079828,2.374360990955564,3.941407786079434,2.500589854502556,3.572158867479355,3.8198977585528904,3.140778607943374,3.057019268580417,3.0456154148643333,3.9378686590640974,3.1698780967361384,2.3432953204876132,3.1557215886747936,3.7054659850570193,2.8552890287062525,3.044828942194259,3.4144710971293746,3.4939048368069208,2.372788045615415,4.031065670467951,2.1612268973653164,3.4817145104207627,2.066850176956351,3.7235548564687377,3.0829728666928826,4.293354305937869,3.6920959496657493\n" +
                "3.4502660663119116,1.9516987310683587,3.805157593123209,2.503888661481785,4.016782644289807,1.933688088415882,3.634465820712239,2.7822349570200573,3.757265656979124,3.0716332378223496,3.71142038477282,3.0233319688907083,4.168235775685632,2.4654113794514942,3.264019647973803,3.4081047891936143,3.7314776913630783,3.3827261563651247,3.6602537863282847,3.0814572247237004,2.2181743757674988,4.208350388866148,2.810478919361441,4.078591895210806,2.125255832992223,3.8563241915677446,1.9545640605812526,3.8084322554236594,3.993041342611543,3.807613589848547,2.8866148178469095,3.6246418338108883,3.819484240687679,3.2361850184199756,2.1956610724519035,3.5480966025378633,3.404420794105608,2.858370855505526,2.9255014326647566,3.415882112157184,3.735571019238641,2.206303724928367,4.1702824396234135,2.0716332378223496,3.8776094965206713,1.837904216127712,3.928776094965207,3.367171510437986,4.074498567335244,4.088825214899713\n" +
                "3.2848200312989047,2.071987480438185,4.221830985915493,2.4389671361502345,4.136932707355243,1.767605633802817,3.6948356807511735,3.0050860719874803,3.501173708920188,2.7758215962441315,2.6291079812206575,3.6396713615023475,3.3971048513302033,3.1412363067292643,2.3215962441314555,2.0735524256651017,2.242566510172144,1.7902973395931143,2.2797339593114243,1.8067292644757433,1.8626760563380282,4.461658841940532,1.698356807511737,4.351330203442879,1.7625195618153364,4.032472613458529,1.5567292644757433,4.128325508607198,4.242566510172144,4.150234741784038,3.776995305164319,2.424491392801252,4.231611893583724,1.8352895148669797,3.270344287949922,2.149452269170579,3.961267605633803,1.9280125195618154,3.7316118935837244,3.895148669796557,3.7715179968701094,1.914319248826291,4.02621283255086,1.939358372456964,4.0481220657277,1.6909233176838812,4.199530516431925,3.0852895148669797,4.0641627543036,4.178403755868545\n" +
                "1.627906976744186,1.372093023255814,1.0930232558139534,0.5581395348837209,0.6046511627906976,0.3953488372093023,0.13953488372093023,0.13953488372093023,0.046511627906976744,0.046511627906976744,1.069767441860465,1.1162790697674418,0.6046511627906976,0.4883720930232558,0.46511627906976744,0.06976744186046512,0.046511627906976744,0.023255813953488372,0.023255813953488372,0.023255813953488372,1.4186046511627908,1.5813953488372092,0.5348837209302325,0.8372093023255814,0.3953488372093023,0.3023255813953488,0.046511627906976744,0.023255813953488372,0.046511627906976744,0.046511627906976744,1.7209302325581395,0.813953488372093,0.7906976744186046,0.2558139534883721,0.32558139534883723,0.23255813953488372,0.046511627906976744,0.023255813953488372,0.06976744186046512,0.046511627906976744,1.4651162790697674,0.7209302325581395,0.6744186046511628,0.5581395348837209,0.32558139534883723,0.06976744186046512,0.11627906976744186,0.046511627906976744,0.046511627906976744,0.20930232558139536\n";
        /*
        StringBuilder centroids = new StringBuilder();

        for (int c = 0; c < numCentroids; c++) {
            for (int attr = 0; attr < numAttributes; attr++) {
                centroids.append(random.nextDouble() * 5);
                if (attr < numAttributes-1)
                    centroids.append(",");
            }
            centroids.append("\n");
        }

        return centroids.toString();

         */
    }

    public static String chooseCentroids(Path datasetPath, int numCentroids) {
        StringBuilder centroids = new StringBuilder();

        try {
            FileSystem fs = FileSystem.get(new Configuration());

            if (fs.exists(datasetPath)) {
                FSDataInputStream inputStream = fs.open(datasetPath);

                // Count number of samples in the dataset
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                int lines = 0;
                while (reader.readLine() != null)
                    lines++;

                reader.close();

                // Select k random rows indexes
                List<Integer> indexes = new ArrayList<>();
                while (indexes.size() < numCentroids) {
                    int randomInd = random.nextInt(lines);

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

                // Close the input stream when you're done
                inputStream.close();

            } else {
                throw new RuntimeException("File does not exist: " + datasetPath);
            }

            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return centroids.toString();
    }
}
