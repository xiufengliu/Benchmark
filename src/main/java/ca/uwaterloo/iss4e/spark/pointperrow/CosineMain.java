package ca.uwaterloo.iss4e.spark.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Cosine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.hadoop.fs.FileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Copyright (c) 2014 Xiufeng Liu ( xiufeng.liu@uwaterloo.ca )
 * <p/>
 * This file is free software: you may copy, redistribute and/or modify it
 * under the terms of the GNU General Public License version 2
 * as published by the Free Software Foundation.
 * <p/>
 * This file is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

public class CosineMain implements Serializable {
    JavaRDD<String> lines = null;

    public void fetch(JavaSparkContext sc, String source) {
        try {
            FileSystem fs = FileSystem.get(new URI(source), new Configuration());
            Path src = new Path(source);
            if (fs.exists(src)) {
                FileStatus[] lists = fs.listStatus(src);
                readFiles(sc, fs, lists);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public void readFiles(JavaSparkContext sc, FileSystem fs, FileStatus[] files) {
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                try {
                    readFiles(sc, fs, fs.listStatus(files[i].getPath()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                if (lines == null) {
                    Path p = files[i].getPath();
                    lines = sc.textFile(p.toString());
                } else {
                    JavaRDD<String> r = sc.textFile(files[i].getPath().toString());
                    lines.union(r);
                }
            }
        }
    }


    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.spark.pointperrow.CosineMain <inputDir> <outputDir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
                .setAppName("CosineMain")
                .set("spark.shuffle.consolidateFiles", "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        //JavaRDD<String> lines = ctx.textFile(args[0]);
        fetch(ctx, args[0]);

        JavaPairRDD<Integer, Iterable<Double>> stage1 = lines.mapToPair(new PairFunction<String, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(String s) {
                String[] values = s.split(",");
                return new Tuple2<Integer, Double>(Integer.parseInt(values[0]), Double.valueOf(values[2]));

            }
        }).groupByKey();

        List<Tuple2<Double, String>> results = stage1.cartesian(stage1).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Iterable<Double>>, Tuple2<Integer, Iterable<Double>>>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<Tuple2<Integer, Iterable<Double>>, Tuple2<Integer, Iterable<Double>>> pair) throws Exception {
                int meterID1 = pair._1()._1().intValue();
                int meterID2 = pair._2()._1().intValue();
                Iterator<Double> itr1 = pair._1()._2().iterator();
                Iterator<Double> itr2 = pair._2()._2().iterator();
                List<Double> reading1 = new ArrayList<Double>();
                List<Double> reading2 = new ArrayList<Double>();
                while (itr1.hasNext() && itr2.hasNext()) {
                    reading1.add(itr1.next());
                    reading2.add(itr2.next());
                }
                String key = meterID1 + "," + meterID2;
                if (meterID2 < meterID1)
                    key = meterID2 + "," + meterID1;
                Double similarity = Cosine.cosine_similarity(reading1, reading2);
                return similarity == null ? new Tuple2<Double, String>(0.0, key) : new Tuple2<Double, String>(similarity, key);
            }
        }).sortByKey(false).take(20);



        /*
        JavaPairRDD<Integer, Double[]> stage2 = stage1.mapValues(new Function<Iterable<Double>, Double[]>() {
            @Override
            public Double[] call(Iterable<Double> values) throws Exception {
                Iterator<Double> itr = values.iterator();
                List<Double> readings = new ArrayList<Double>();
                while (itr.hasNext()) {
                    readings.add(itr.next());
                }
                return readings.toArray(new Double[]{});
            }
        });

        JavaPairRDD<String, Tuple2<Double[], Double[]>> stage3 = stage2.cartesian(stage2)
                .filter(new Function<Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>> pair) throws Exception {
                        return pair._1()._1().intValue() != pair._2()._1().intValue();
                    }
                }).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>>, String, Tuple2<Double[], Double[]>>() {
                    @Override
                    public Tuple2<String, Tuple2<Double[], Double[]>> call(Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>> pair) throws Exception {
                        int meterID1 = pair._1()._1().intValue();
                        int meterID2 = pair._2()._1().intValue();
                        String key = meterID1 + "," + meterID2;
                        if (meterID2 > meterID1) {
                            key = meterID2 + "," + meterID1;
                        }
                        return new Tuple2<String, Tuple2<Double[], Double[]>>(key, new Tuple2<Double[], Double[]>(pair._1()._2(), pair._2()._2()));
                    }
                }).distinct();

        JavaPairRDD<Double, String> stage4 = stage3.mapValues(new Function<Tuple2<Double[], Double[]>, Double>() {
            @Override
            public Double call(Tuple2<Double[], Double[]> pair) throws Exception {
                return Cosine.cosine_similarity(pair._1(), pair._2());
            }
        }).mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> pair) throws Exception {
                return new Tuple2<Double, String>(pair._2(), pair._1());
            }
        });
        List<Tuple2<Double, String>> results = stage4.sortByKey(false).take(10);
*/

        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < results.size(); i += 2) {
            Tuple2<Double, String> tuple = results.get(i);
            buf.append(tuple._2()).append("=").append(tuple._1()).append("\n");
        }
        System.out.println(buf.toString());
        //results.saveAsTextFile("output");
        ctx.stop();
    }

    public static void main(String[] args) throws Exception {
        CosineMain cosineMain = new CosineMain();
        cosineMain.run(args);
    }
}
