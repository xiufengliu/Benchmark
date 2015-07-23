package ca.uwaterloo.iss4e.spark.abnormaldetect;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Created by xiuli on 7/19/15.
 */
public class DetectByNeighborAvg {

    private static final Logger log = Logger.getLogger(DetectByNeighborAvg.class);

    public static double phi(double x) {
        return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    }

    // return phi(x, mu, signma) = Gaussian pdf with mean mu and stddev sigma
    public static double phi(double x, double mu, double sigma) {
        return phi((x - mu) / sigma) / sigma;
    }

    public static void train(String neighborDataDir, String trainingDataDir, String modelDataDir) {
        SparkConf conf = new SparkConf().setAppName("Training");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> neighborLines = sc.textFile(neighborDataDir);
        final Map<Integer, Iterable<Integer>> myID2Neighbors = neighborLines
                .mapToPair(new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        return new Tuple2<Integer, Integer>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1])); //MyMeterID-->neighbor
                    }
                }).groupByKey().collectAsMap();

        final Map<Integer, Iterable<Integer>> neighbor2MyID = neighborLines
                .mapToPair(new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        return new Tuple2<Integer, Integer>(Integer.valueOf(fields[1]), Integer.valueOf(fields[0])); //neighbor-->myID
                    }
                }).groupByKey().collectAsMap();
        sc.broadcast(myID2Neighbors);
        sc.broadcast(neighbor2MyID);


        final JavaPairRDD<String, String> trainRDD = sc.textFile(trainingDataDir).mapToPair(new PairFunction<String, Tuple2<Integer, Integer>, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> call(String line) throws Exception {
                String[] fields = line.split("\\|");
                int meterID = Integer.valueOf(fields[0]);
                String readtime = fields[1]; // 2012-01-01 02:00:00
                readtime = readtime.replace("-", "");
                String[] ts = readtime.split(" ");
                int readdate = Integer.valueOf(ts[0]);
                String[] hs = ts[1].split(":");
                int hour = Integer.valueOf(hs[0]);
                double reading = Double.valueOf(fields[2]);
                return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>(new Tuple2<Integer, Integer>(meterID, readdate), new Tuple2<Integer, Double>(hour, reading)); //(myMeterID, readdate)->(hour, Reading)
            }
        }).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double>>>, Tuple2<Integer, Integer>, Tuple2<Integer, Double[]>>() {
            @Override
            public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double[]>>> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double>>> tuple) throws Exception {
                int meterID = tuple._1._1;
                int readdate = tuple._1._2;
                Iterator<Tuple2<Integer, Double>> itr = tuple._2.iterator();
                Double[] readings = new Double[24];
                while (itr.hasNext()) {
                    Tuple2<Integer, Double> hourReading = itr.next();
                    readings[hourReading._1] = hourReading._2;
                }

                List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double[]>>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double[]>>>();
                if (myID2Neighbors.containsKey(meterID)) {
                    ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double[]>>(new Tuple2<Integer, Integer>(meterID, readdate), new Tuple2<Integer, Double[]>(meterID, readings)));
                }
                if (neighbor2MyID.containsKey(meterID)) {
                    Iterator<Integer> itr2 = neighbor2MyID.get(meterID).iterator();
                    while (itr2.hasNext()) {
                        Integer hostMeterID = itr2.next();
                        ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double[]>>(new Tuple2<Integer, Integer>(hostMeterID, readdate), new Tuple2<Integer, Double[]>(meterID, readings)));
                    }
                }
                return ret;
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double[]>>>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double[]>>> tuple) throws Exception {
                int myMeterID = tuple._1._1.intValue();
                //  int readdate = tuple._1._2;
                double[] myReading = new double[24];
                double[] myNeighReading = new double[24];
                int neighCount = 0;
                Iterator<Tuple2<Integer, Double[]>> itr = tuple._2.iterator();
                while (itr.hasNext()) {
                    Tuple2<Integer, Double[]> readings = itr.next(); //(neighborMeterID, hour, Reading)
                    int neighMeterID = readings._1().intValue();
                    Double[] h24readings = readings._2();
                    if (myMeterID == neighMeterID) {
                        for (int i = 0; i < 24; ++i) {
                            myReading[i] = h24readings[i].doubleValue();
                        }
                    } else {
                        boolean valid = true;
                        for (int i = 0; i < 24; ++i) {
                            Double value = h24readings[i];
                            if (value != null) {
                                myNeighReading[i] += value.doubleValue();
                            } else {
                                valid = false;
                                break;
                            }
                        }
                        if (valid) ++neighCount;
                    }
                }
                double l2dist = 0;
                for (int i = 0; i < 24; ++i) {
                    l2dist += Math.pow((myReading[i] - myNeighReading[i] / neighCount), 2);
                }
                return new Tuple2<Integer, Double>(myMeterID, Math.sqrt(l2dist));
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Integer, Iterable<Double>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Integer, Iterable<Double>> tuple) throws Exception {
                int meterID = tuple._1;
                Iterator<Double> itr = tuple._2.iterator();
                List<Double> list = new ArrayList<Double>();
                double sum = 0.0;
                double variance = 0.0;
                while (itr.hasNext()) {
                    double value = itr.next().doubleValue();
                    list.add(value);
                    sum += value;
                }
                int size = list.size();
                double mean = sum / size;
                for (double v : list) {
                    variance += Math.pow((v - mean), 2);
                }
                return new Tuple2<String, String>(String.valueOf(meterID), mean + "\t" + Math.sqrt(variance / size));
            }
        });
        trainRDD.saveAsNewAPIHadoopFile(modelDataDir, Text.class, Text.class, TextOutputFormat.class);
        sc.stop();
    }

    public static void detect(String modelDir, String neighborDataDir, String checkPointDir, String streamDataDir, final String outputDataDir, int duration, final double epsilon) {
        SparkConf conf = new SparkConf().setAppName("AbnormalDetection");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> modelLines = sc.textFile(modelDir);
        final Map<Integer, double[]> models = modelLines
                .mapToPair(new PairFunction<String, Integer, double[]>() {
                    @Override
                    public Tuple2<Integer, double[]> call(String line) throws Exception {
                        String[] vales = line.split("\t");
                        return new Tuple2<Integer, double[]>(Integer.valueOf(vales[0]), new double[]{Double.valueOf(vales[1]), Double.valueOf(vales[2])});
                    }
                }).collectAsMap();
        sc.broadcast(models);

        JavaRDD<String> neighborLines = sc.textFile(neighborDataDir);
        final Map<Integer, Iterable<Integer>> myID2Neighbors = neighborLines
                .mapToPair(new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        return new Tuple2<Integer, Integer>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1])); //MyMeterID-->neighbor
                    }
                }).groupByKey().collectAsMap();

        final Map<Integer, Iterable<Integer>> neighbor2MyID = neighborLines
                .mapToPair(new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        return new Tuple2<Integer, Integer>(Integer.valueOf(fields[1]), Integer.valueOf(fields[0])); //neighbor-->myID
                    }
                }).groupByKey().collectAsMap();
        sc.broadcast(myID2Neighbors);
        sc.broadcast(neighbor2MyID);


        final JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(duration));
        ssc.checkpoint(checkPointDir);
        JavaDStream<String> testLines = ssc.textFileStream(streamDataDir);

        JavaPairDStream<String, String> results =
                testLines.mapToPair(new PairFunction<String, Tuple2<Integer, Integer>, Tuple2<Integer, Double>>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        int meterID = Integer.valueOf(fields[0]);
                        String readtime = fields[1]; // 2012-01-01 02:00:00
                        readtime = readtime.replace("-", "");
                        String[] ts = readtime.split(" ");
                        int readdate = Integer.valueOf(ts[0]);
                        String[] hs = ts[1].split(":");
                        int hour = Integer.valueOf(hs[0]);
                        double reading = Double.valueOf(fields[2]);
                        return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>(new Tuple2<Integer, Integer>(meterID, readdate), new Tuple2<Integer, Double>(hour, reading)); //(myMeterID, readdate)->(hour, Reading)
                    }
                }).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double>>>, Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double>>> tuple) throws Exception {
                        int meterID = tuple._1._1;
                        int readdate = tuple._1._2;
                        Iterator<Tuple2<Integer, Double>> itr = tuple._2.iterator();
                        double[] readings = new double[24];
                        while (itr.hasNext()) {
                            Tuple2<Integer, Double> hourReading = itr.next();
                            readings[hourReading._1] = hourReading._2;
                        }

                        List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>>();
                        if (myID2Neighbors.containsKey(meterID)) {
                            ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>(new Tuple2<Integer, Integer>(meterID, readdate), new Tuple2<Integer, double[]>(meterID, readings)));
                        }
                        if (neighbor2MyID.containsKey(meterID)) {
                            Iterator<Integer> itr2 = neighbor2MyID.get(meterID).iterator();
                            while (itr2.hasNext()) {
                                Integer hostMeterID = itr2.next();
                                ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>(new Tuple2<Integer, Integer>(hostMeterID, readdate), new Tuple2<Integer, double[]>(meterID, readings)));
                            }
                        }
                        return ret;
                    }
                }).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, double[]>>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, double[]>>> tuple) throws Exception {
                        int myMeterID = tuple._1._1;
                        int readdate = tuple._1._2;
                        double[] myReading = new double[24];
                        double[] myNeighReading = new double[24];
                        int neighCount = 0;
                        Iterator<Tuple2<Integer, double[]>> itr = tuple._2.iterator();
                        while (itr.hasNext()) {
                            Tuple2<Integer, double[]> neighReadings = itr.next(); //(neighborMeterID, hour, Reading)
                            int neighMeterID = neighReadings._1().intValue();
                            double[] h24readings = neighReadings._2();
                            if (myMeterID == neighMeterID) {
                                myReading = h24readings;
                            } else {
                                boolean valid = true;
                                for (int i = 0; i < 24; ++i) {
                                    Double value = h24readings[i];
                                    if (value != null) {
                                        myNeighReading[i] += value.doubleValue();
                                    } else {
                                        valid = false;
                                        break;
                                    }
                                }
                                if (valid) ++neighCount;
                            }
                        }
                        double dist = 0;
                        for (int i = 0; i < 24; ++i) {
                            dist += Math.pow((myReading[i] - myNeighReading[i] / neighCount), 2);
                        }
                        dist = Math.sqrt(dist);
                        double[] param = models.get(myMeterID);
                        double probability = phi(dist, param[0], param[1]);
                        List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
                        if (probability < epsilon)
                            ret.add(new Tuple2<String, String>(String.valueOf(myMeterID), readdate + "\t" + probability));
                        return ret;
                    }
                });

        results.foreachRDD(new Function2<JavaPairRDD<String, String>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> rdd, Time time) throws Exception {
                if (!rdd.isEmpty()) {
                    String file = outputDataDir + "/output" + "-" + time.milliseconds();
                    rdd.saveAsNewAPIHadoopFile(file, Text.class, Text.class, TextOutputFormat.class);
                }
                return null;
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }

    public static void main(String[] args) {
        String usage = "DetectByNeighborAvg [train|test] <neighborDataDir> <trainingDataDir> <modelDataDir> <checkPointDir> <streamDataDir> <outputDataDir> <durationInSeconds> <epsilon>";
        if (args.length != 9) {
            System.out.println(usage);
        }
        String neighborDataDir = args[1];
        String trainingDataDir = args[2];
        String modelDataDir = args[3];
        String checkPointDir = args[4];
        String streamDataDir = args[5];
        String outputDataDir = args[6];
        int durationInSeconds = Integer.valueOf(args[7]);
        double epsilon = Double.valueOf(args[8]);
        if ("train".equals(args[0])) {
            DetectByNeighborAvg.train(neighborDataDir, trainingDataDir, modelDataDir);
        } else {
            DetectByNeighborAvg.detect(modelDataDir, neighborDataDir, checkPointDir, streamDataDir, outputDataDir, durationInSeconds, epsilon);
        }

    }
}
