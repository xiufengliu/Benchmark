package ca.uwaterloo.iss4e.spark.meterperrow;

import ca.uwaterloo.iss4e.algorithm.Histogram;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
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

public class HistogramMain implements Serializable {
    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.spark.meterperrow.HistogramMain <inputDir> <outputDir>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("HistogramMain")
                .set("spark.shuffle.consolidateFiles", "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0]);

        JavaPairRDD<Integer, int[]> consumptions = lines.mapToPair(new PairFunction<String, Integer, int[]>() {
            @Override
            public Tuple2<Integer, int[]> call(String s) {
                String[] values = s.split(",");
                Integer meterID = Integer.valueOf(values[0]);

                String[] readings = values[1].split(";");
                List<Double> readingList = new ArrayList<Double>();
                for (int i = 0; i < readings.length; ++i) {
                    readingList.add(Double.valueOf(readings[i]));
                }
                int[] hist = Histogram.calcHistogram(readingList, 10);
                return new Tuple2<Integer, int[]>(meterID, hist);

            }
        });
        consumptions.saveAsTextFile(args[1]);
        ctx.stop();
    }

    public static void main(String[] args) throws Exception {
        HistogramMain histogramMain = new HistogramMain();
        histogramMain.run(args);
    }

}
