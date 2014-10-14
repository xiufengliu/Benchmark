package ca.uwaterloo.iss4e.spark.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Histogram;
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

public class HistogramMain implements Serializable {


    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.spark.pointperrow.HistogramMain <inputDir> <outputDir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
                .setAppName("HistogramMain")
                .set("spark.shuffle.consolidateFiles",  "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(args[0]);

        JavaPairRDD<Integer, Iterable<Double>> stage1 = lines.mapToPair(new PairFunction<String, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(String s) {
                String[] values = s.split(",");
                return new Tuple2<Integer, Double>(Integer.parseInt(values[0]), Double.valueOf(values[2]));

            }
        }).groupByKey();

        JavaPairRDD<Integer, int[]> results = stage1.mapValues(new Function<Iterable<Double>, int[]>() {
            @Override
            public int[] call(Iterable<Double> values) throws Exception {
                Iterator<Double> itr = values.iterator();
                List<Double> readings = new ArrayList<Double>();
                while (itr.hasNext()) {
                    readings.add(itr.next());
                }
                Collections.sort(readings);
                return Histogram.calcHistogram(readings, 10);
            }
        });
        results.saveAsTextFile(args[1]);

        ctx.stop();
    }

    public static void main(String[] args) throws Exception {
        HistogramMain histogramMain = new HistogramMain();
        histogramMain.run(args);
    }
}
