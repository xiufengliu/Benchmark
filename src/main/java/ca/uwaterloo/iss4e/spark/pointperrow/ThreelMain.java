package ca.uwaterloo.iss4e.spark.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Threelines;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 *
 *  Copyright (c) 2014 Xiufeng Liu ( xiufeng.liu@uwaterloo.ca )
 *
 *  This file is free software: you may copy, redistribute and/or modify it
 *  under the terms of the GNU General Public License version 2
 *  as published by the Free Software Foundation.
 *
 *  This file is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses.
 */

public final class ThreelMain implements Serializable {

    public static class MyIterator<T> implements Iterable<T> {
        T value;

        public MyIterator(T value) {
            this.value = value;
        }

        @Override
        public Iterator<T> iterator() {
            List<T> list = new ArrayList<T>();
            if (this.value != null) {
                list.add(this.value);
            }
            return list.iterator();
        }
    }

    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.spark.pointperrow.ThreelMain <inputDir> <outputDir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
                .setAppName("ThreelMain")
                .set("spark.shuffle.consolidateFiles",  "true");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        final long startTime = System.currentTimeMillis();
        JavaRDD<String> lines = ctx.textFile(args[0]);
        lines.cache();
        final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Duration of caching is " + duration + " seconds.");

        JavaPairRDD<String, Iterable<Double>> stage1 = lines.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) {
                String[] values = s.split(",");
                return new Tuple2(values[0] + "," + Math.round(Float.parseFloat(values[3])), Double.valueOf(values[2]));
            }
        }).groupByKey();


        JavaPairRDD<String, Double> stage2 = stage1.flatMapValues(new Function<Iterable<Double>, Iterable<Double>>() {
            @Override
            public Iterable<Double> call(Iterable<Double> values) throws Exception {
                Iterator<Double> itr = values.iterator();
                List<Double> readings = new ArrayList<Double>();
                while (itr.hasNext()) {
                    Double val = itr.next();
                    readings.add(val);
                }
                Double value = (readings.size() < 20) ? null : Threelines.median(readings);
                return new MyIterator<Double>(value);
            }
        });

        JavaPairRDD<Integer, Iterable<double[]>> stage3 = stage2.mapToPair(new PairFunction<Tuple2<String, Double>, Integer, double[]>() {
            @Override
            public Tuple2<Integer, double[]> call(Tuple2<String, Double> kv) throws Exception {
                String[] s = kv._1().split(",");
                return new Tuple2<Integer, double[]>(Integer.valueOf(s[0]), new double[]{Double.valueOf(s[1]), kv._2()});
            }
        }).groupByKey();


        JavaPairRDD<Integer, String> results = stage3.mapValues(new Function<Iterable<double[]>, String>() {
            @Override
            public String call(Iterable<double[]> values) throws Exception {
                TreeMap<Double, Double> tempMap = new TreeMap<Double, Double>();
                Iterator<double[]> itr = values.iterator();
                while (itr.hasNext()) {
                    double[] vals = itr.next();
                    tempMap.put(vals[0], vals[1]);
                }
                double[][] points = Threelines.threel(tempMap);
                StringBuffer buf = new StringBuffer();
                for (int i = 0; i < points.length; ++i) {
                    buf.append(points[i][0]).append(",").append(points[i][1]).append(",");
                }
                return buf.toString();
            }
        });
        results.saveAsTextFile(args[1]);
        ctx.stop();
    }

    public static void main(String[] args) throws Exception {
        ThreelMain threelMain = new ThreelMain();
        threelMain.run(args);
    }
}