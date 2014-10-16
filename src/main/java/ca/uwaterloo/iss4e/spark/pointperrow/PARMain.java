package ca.uwaterloo.iss4e.spark.pointperrow;

import ca.uwaterloo.iss4e.algorithm.PAR;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

public class PARMain implements Serializable {

    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.spark.pointperrow.PARMain <inputDir> <outputDir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
                .setAppName("PARMain")
                .set("spark.shuffle.consolidateFiles",  "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        final long startTime = System.currentTimeMillis();
        JavaRDD<String> lines = ctx.textFile(args[0]);
        lines.cache();
        final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Duration of caching is " + duration + " seconds.");

        JavaPairRDD<Integer, Iterable<Double>> stage1 = lines.mapToPair(new PairFunction<String, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(String s) {
                String[] values = s.split(",");
                return new Tuple2<Integer, Double>(Integer.parseInt(values[0]), Double.valueOf(values[2]));

            }
        }).groupByKey();

        JavaPairRDD<Integer, String> results = stage1.mapValues(new Function<Iterable<Double>, String>() {
            @Override
            public String call(Iterable<Double> values) throws Exception {
                Iterator<Double> itr = values.iterator();
                List<Double> readings = new ArrayList<Double>();
                while (itr.hasNext()) {
                    readings.add(itr.next());
                }
                Double[] readingArray = readings.toArray(new Double[]{});
                Double[][] parArray = PAR.computeParameters(readingArray, 3, 24);

                StringBuffer buf = new StringBuffer();
                for (int i = 0; i < parArray.length; ++i) {
                    Double[] pars = parArray[i];
                    for (Double p : pars) {
                        buf.append(p.doubleValue()).append(",");
                    }
                }
                return buf.toString();
            }
        });
        results.saveAsTextFile(args[1]);
        ctx.stop();
    }

    public static void main(String[] args) throws Exception {
        PARMain parMain = new PARMain();
        parMain.run(args);
    }
}
