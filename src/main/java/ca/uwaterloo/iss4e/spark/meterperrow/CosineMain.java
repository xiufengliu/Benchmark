package ca.uwaterloo.iss4e.spark.meterperrow;

import ca.uwaterloo.iss4e.algorithm.Cosine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
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
    public static void main(String[] args) throws Exception {
        CosineMain cosineMain = new CosineMain();
        cosineMain.run(args);
    }

    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.spark.meterperrow.CosineMain <inputDir> <outputDir>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("CosineMain")
                .set("spark.shuffle.consolidateFiles", "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0]);

        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<Integer, Double[]> consumptions1 = lines.mapToPair(new PairFunction<String, Integer, Double[]>() {
            @Override
            public Tuple2<Integer, Double[]> call(String s) {
                String[] values = s.split(",");
                Integer meterID = Integer.valueOf(values[0]);
                String[] readings = values[2].split(";");

                Double[] readingArray = new Double[readings.length];
                for (int i = 0; i < readings.length; ++i) {
                    readingArray[i] = Double.valueOf(readings[i]);
                }
                return new Tuple2<Integer, Double[]>(meterID, readingArray);
            }
        });

        JavaPairRDD<String, Double> consumption3 = consumptions1.cartesian(consumptions1)
                .filter(new Function<Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>> pair) throws Exception {
                        return pair._1()._1().intValue() != pair._2()._1().intValue();
                    }
                }).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<Tuple2<Integer, Double[]>, Tuple2<Integer, Double[]>> pair) throws Exception {
                        int meterID1 = pair._1()._1().intValue();
                        int meterID2 = pair._2()._1().intValue();
                        String key = meterID1 + "," + meterID2;
                        if (meterID2 > meterID1) {
                            key = meterID2 + "," + meterID1;
                        }
                        Double similarity = Cosine.cosine_similarity(pair._1()._2(), pair._2()._2());
                        return new Tuple2<String, Double>(key, similarity);
                    }
                }).distinct();

        JavaPairRDD<Double, String> consumption4 = consumption3.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> pair) throws Exception {
                return new Tuple2<Double, String>(pair._2(), pair._1());
            }
        });

        List<Tuple2<Double, String>> results = consumption4.sortByKey(false).take(10);
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < results.size(); ++i) {
            Tuple2<Double, String> tuple = results.get(i);
            buf.append(tuple._2()).append("=").append(tuple._1()).append("\n");
        }
        System.out.println(">>>>>>>>>>>>>>>>>" + buf.toString());
        //  results.saveAsTextFile("output");

        // Collects all URL ranks and dump them to console.
       /* Map<Integer, int[]> output = results.collectAsMap();

        StringBuffer buf = new StringBuffer();
        int count = 0;
        for (Map.Entry<Integer, int[]> kv : output.entrySet()) {
            ++count;
            buf.setLength(0);
            buf.append(kv.getKey()).append("=");
            int[] bins = kv.getValue();
            for (int i = 0; i < bins.length; ++i) {
                buf.append("(").append(i).append(",").append(bins[i]).append(");");
            }
            System.out.print(buf.toString());
        }*/
        ctx.stop();
    }

}
