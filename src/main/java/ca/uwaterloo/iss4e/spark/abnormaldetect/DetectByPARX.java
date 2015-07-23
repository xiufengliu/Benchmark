package ca.uwaterloo.iss4e.spark.abnormaldetect;

/**
 * Created by xiuli on 6/27/15.
 */

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class DetectByPARX {

    static int getHour(String timestamp) {
        Date date = null;
        try {
            date = DateUtils.parseDate(timestamp, new String[]{"yyyy-MM-dd HH:mm:ss"});
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    static double getDays(String timestamp) {
        String[] s = timestamp.split(" ");
        try {
            Date date1 = DateUtils.parseDate("1970-01-01", new String[]{"yyyy-MM-dd"});
            Date date2 = DateUtils.parseDate(s[0], new String[]{"yyyy-MM-dd"});
            return TimeUnit.DAYS.convert(date2.getTime() - date1.getTime(), TimeUnit.MILLISECONDS) * 1.0d;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0.0;
    }

    static String getDate(double days) {
        try {
            Date epocheDate = DateUtils.parseDate("1970-01-01", new String[]{"yyyy-MM-dd"});
            Date theDate = DateUtils.addDays(epocheDate, (int) days);
            return DateFormatUtils.format(theDate, "yyyy-MM-dd");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return "";
    }

    static class Split implements PairFunction<String, Tuple2<Integer, Integer>, double[]> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, double[]> call(String line) throws Exception {
            String[] values = line.split("\\|");
            int[] key = new int[2];
            double[] value = new double[2];

            key[0] = Integer.valueOf(values[0]);
            key[1] = Integer.valueOf(DetectByPARX.getHour(values[1]));
            value[0] = Double.parseDouble(values[2]);
            value[1] = Double.parseDouble(values[3]);
            return new Tuple2<Tuple2<Integer, Integer>, double[]>(new Tuple2(Integer.valueOf(values[0]), Integer.valueOf(DetectByPARX.getHour(values[1]))), value);
        }
    }

    static class Split2 implements PairFunction<String, Tuple2<Integer, Integer>, double[]> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, double[]> call(String line) throws Exception {
            String[] values = line.split("\\|");
            int[] key = new int[2];
            double[] value = new double[3];// reading, temperature, and date

            key[0] = Integer.valueOf(values[0]);
            key[1] = Integer.valueOf(DetectByPARX.getHour(values[1])); // MeterID, season (hour of the day)
            value[0] = Double.parseDouble(values[2]);
            value[1] = Double.parseDouble(values[3]);
            value[2] = getDays(values[1]);
            return new Tuple2<Tuple2<Integer, Integer>, double[]>(new Tuple2(Integer.valueOf(values[0]), Integer.valueOf(DetectByPARX.getHour(values[1]))), value);
        }
    }

    static class PARXM implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<double[]>>, Tuple2<Integer, Integer>, double[]> {


        @Override
        public Tuple2<Tuple2<Integer, Integer>, double[]> call(Tuple2<Tuple2<Integer, Integer>, Iterable<double[]>> tuple) throws Exception {
            Iterable<double[]> readingTempItr = tuple._2();
            Iterator<double[]> itr = readingTempItr.iterator();
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            List<Double> yArray = new ArrayList<Double>();
            List<double[]> xMatrix = new ArrayList<double[]>();
            double[] window = new double[4];
            int order = 3;

            CircularArrayList win = new CircularArrayList<Double>(order);
            while (itr.hasNext()) {
                double[] readingTemp = itr.next();
                double reading = readingTemp[0];
                double temp = readingTemp[1];
                if (win.isFull()) {
                    double[] x = new double[6];
                    yArray.add(reading);
                    for (int i = 0; i < order; ++i)
                        x[i] = (double) win.get(win.size() - i - 1);

                    x[order] = temp > 20 ? temp - 20 : 0;
                    x[order + 1] = temp < 16 ? 16 - temp : 0;
                    x[order + 2] = temp < 5 ? 5 - temp : 0;
                    xMatrix.add(x);
                    win.remove(0);
                    win.add(reading);
                } else {
                    win.add(reading);
                }
            }
            Double[] Y = yArray.toArray(new Double[]{});
            double[] y = ArrayUtils.toPrimitive(Y);
            double[][] X = xMatrix.toArray(new double[][]{});
            try {
                regression.newSampleData(y, X);
                double[] parameters = regression.estimateRegressionParameters();
                return new Tuple2<Tuple2<Integer, Integer>, double[]>(tuple._1, parameters);
            }catch (Exception e){
                return new Tuple2<Tuple2<Integer, Integer>, double[]>(tuple._1, new double[]{0.0,0.0,0.0,0.0,0.0,0.0,0.0});
           }
        }
    }


    static class Predict implements PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<double[]>, double[]>>, Tuple2<Integer, Integer>, Double> {
        @Override
        public Iterable<Tuple2<Tuple2<Integer, Integer>, Double>> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<double[]>, double[]>> tuple) throws Exception {
            Iterator<double[]> itr = tuple._2._1.iterator();
            double[] parameters = tuple._2._2;
            int order = 3;
            CircularArrayList<Double> win = new CircularArrayList<Double>(order);
            List<Double> predictValues = new ArrayList<Double>();
            List<Tuple2<Tuple2<Integer, Integer>, Double>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();
            while (itr.hasNext()) {
                double[] readingTempDays = itr.next();
                double reading = readingTempDays[0];
                double temp = readingTempDays[1];
                int days = (int) readingTempDays[2];
                if (win.isFull()) {
                    double predictReading = parameters[0]
                            + parameters[1] * win.get(win.size() - 1) + parameters[2] * win.get(win.size() - 2) + parameters[3] * win.get(win.size() - 3)
                            + parameters[4] * (temp > 20 ? temp - 20 : 0) + parameters[5] * (temp < 16 ? 16 - temp : 0) + parameters[6] * (temp < 5 ? 5 - temp : 0);
                    ret.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(tuple._1._1, days), (reading-predictReading)*(reading-predictReading)));
                    win.remove(0);
                    win.add(reading);
                } else {
                    win.add(reading);
                }
            }
            return ret;// (meterID, days): norm2
        }
    }

    static class EuclideanDistance implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double norm1, Double norm2) throws Exception {
            return norm1.doubleValue()+norm2.doubleValue();
        }
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("java DetectByPARX <checkPointDir> <trainingDataDir> <streamDataDir> <DurationInSeconds> <epsilon>");
            return;
        }
        String checkPointDir = args[0];
        String trainingDataDir = args[1];
        String streamDataDir = args[2];
        int duration = Integer.parseInt(args[3]);
        final double epsilon = Double.parseDouble(args[4]);

        SparkConf conf = new SparkConf().setAppName("AbnormalDetection");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> trainingLines = sc.textFile(trainingDataDir);
        final JavaPairRDD<Tuple2<Integer, Integer>, double[]> paramRDD = trainingLines
                .mapToPair(new Split())
                .groupByKey()
                .mapToPair(new PARXM())
                .cache();



        final JavaPairRDD<Integer,  double[]> normalDist = trainingLines
                .mapToPair(new Split2())
                .groupByKey()
                .join(paramRDD)
                .flatMapToPair(new Predict())
                .reduceByKey(new EuclideanDistance())
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer,Integer>,Double>, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Tuple2<Integer, Integer>, Double> tuple) throws Exception {
                        return new Tuple2<Integer, Double>(tuple._1._1, Math.sqrt(tuple._2));
                    }
                })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<Integer, Iterable<Double>>, Integer, double[]>() {
                    @Override
                    public Tuple2<Integer, double[]> call(Tuple2<Integer, Iterable<Double>> tuple) throws Exception {
                        Iterator<Double> itr = tuple._2.iterator();
                        List<Double> distances = new ArrayList<Double>();
                        double sum = 0.0;
                        while (itr.hasNext()) {
                            double dist = itr.next().doubleValue();
                            sum += dist;
                            distances.add(dist);
                        }
                        double mean = sum / distances.size();
                        sum = 0.0;
                        for (double dist : distances) {
                            sum += (dist - mean) * (dist - mean);
                        }
                        double thetaSquare = sum / distances.size();
                        return new Tuple2<Integer, double[]>(tuple._1, new double[]{mean, thetaSquare});
                    }
                }).cache();

       final JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(duration));
        ssc.checkpoint(checkPointDir);

        JavaDStream<String> testLines = ssc.textFileStream(streamDataDir);

        JavaPairDStream<String, String> results =
                testLines.mapToPair(new Split2())
                .groupByKeyAndWindow(Durations.seconds(4*duration), Durations.seconds(duration))
                .transformToPair(new Function<JavaPairRDD<Tuple2<Integer, Integer>, Iterable<double[]>>, JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Iterable<double[]>, double[]>>>() {
                    @Override
                    public JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Iterable<double[]>, double[]>> call(JavaPairRDD<Tuple2<Integer, Integer>, Iterable<double[]>> rdd) throws Exception {
                        return rdd.join(paramRDD);
                    }
                }).flatMapToPair(new Predict())
                .reduceByKey(new EuclideanDistance())
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Double>, Integer, Tuple2<Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<Integer, Double>> call(Tuple2<Tuple2<Integer, Integer>, Double> tuple) throws Exception {
                        return new Tuple2<Integer, Tuple2<Integer, Double>>(tuple._1._1, new Tuple2<Integer, Double>(tuple._1._2, Math.sqrt(tuple._2)));//meterid:(days, norm):
                    }
                }).groupByKey()
                .transformToPair(new Function<JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>>, JavaPairRDD<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, double[]>>>() {
                    @Override
                    public JavaPairRDD<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, double[]>> call(JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> rdd) throws Exception {
                        return rdd.join(normalDist);
                    }
                }).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, double[]>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, double[]>> tuple) throws Exception {
                        List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
                        int meterID = tuple._1;
                        double mean = tuple._2._2[0];
                        double thetaSquare = tuple._2._2[1];
                        Iterator<Tuple2<Integer, Double>> itr = tuple._2._1.iterator();
                        while (itr.hasNext()){
                            Tuple2<Integer, Double> dayDist = itr.next();
                            String date = getDate(dayDist._1);
                            double x = dayDist._2;
                            double propability = Math.exp(-Math.pow(x - mean, 2) / (2 * thetaSquare)) / (Math.sqrt(2 * Math.PI * thetaSquare));
                            if (propability < epsilon){
                                ret.add(new Tuple2<String, String>((meterID +"," + date), String.valueOf(propability)));
                            }
                        }
                        return ret;
                    }
                });
        results.foreachRDD(new Function2<JavaPairRDD<String, String>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> rdd, Time time) throws Exception {
                if (!rdd.isEmpty())  {
                    String file = "/user/xiuli/output" + "-" + time.milliseconds();
                    rdd.saveAsNewAPIHadoopFile(file, Text.class, Text.class, TextOutputFormat.class);
                }
                return null;
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
