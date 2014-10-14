package ca.uwaterloo.iss4e.hadoop.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Threelines;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
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

public class ThreelMain extends Configured implements Tool {
    public static final String DESCRIPTION = "Threeline regression program";

    private static final Log LOG = LogFactory.getLog(ThreelMain.class);

    public static class MyMapper extends  Mapper<Object, Text, LongWritable, ArrayPrimitiveWritable> {

        @Override
        protected void map(Object offset, Text line,  final Context context) throws IOException, InterruptedException {
            String[] fieldValues = line.toString().split(",");
            context.write(new LongWritable(Long.valueOf(fieldValues[0])), new ArrayPrimitiveWritable(new double[]{
                    Double.parseDouble(fieldValues[2]), Double.parseDouble(fieldValues[3])
            }) ); // reading, temperature
        }
    }

    public static class MyCombiner extends  Reducer<LongWritable, ArrayPrimitiveWritable, LongWritable, ArrayPrimitiveWritable> {

        @Override
        protected void reduce(LongWritable meterID, Iterable<ArrayPrimitiveWritable> values,
                              Context context) throws IOException, InterruptedException {
            Iterator<ArrayPrimitiveWritable> itr = values.iterator();
            List<Double> readingTempPairs = new ArrayList<Double>();
            while (itr.hasNext()){
                ArrayPrimitiveWritable value = itr.next();
                double[] readingTemp =  (double[])value.get();
                readingTempPairs.add(readingTemp[0]);
                readingTempPairs.add(readingTemp[1]);
            }
            double[] readingTempArray = new double[readingTempPairs.size()];
            for (int i=0; i<readingTempPairs.size(); ++i){
                readingTempArray[i] = readingTempPairs.get(i).doubleValue();
            }
            context.write(meterID, new ArrayPrimitiveWritable(readingTempArray));
        }
    }

    public static class MyReducer extends  Reducer<LongWritable, ArrayPrimitiveWritable, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable meterID, Iterable<ArrayPrimitiveWritable> values,
                              Context context) throws IOException, InterruptedException {
            List<Double> readings = new ArrayList<Double>();
            List<Double> temperatures = new ArrayList<Double>();
            Iterator<ArrayPrimitiveWritable> itr= values.iterator();
            while(itr.hasNext()){
                ArrayPrimitiveWritable arr = itr.next();
                double[] readingTemps = (double[])arr.get();
                for (int i=0; i<readingTemps.length/2; ++i) {
                    readings.add(readingTemps[2*i]);
                    temperatures.add(readingTemps[2*i+1]);
                }
            }

            double[][] points = Threelines.threel(temperatures, readings);
            if (points!=null) {
                StringBuffer results = new StringBuffer();
                for (int i = 0; i < points.length; ++i) {
                    double[] point = points[i];
                    results.append(point[0]).append(",").append(point[1]).append("\t");
                }
                context.write(meterID, new Text(results.toString()));
            }
        }
  }

    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.hadoop.pointperrow.ThreelMain <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "ThreelMain");
        job.setJarByClass(ThreelMain.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);

        job.setCombinerClass(MyCombiner.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.out.println("\nStarting Job ...");
        final long startTime = System.currentTimeMillis();
        try {
            if (!job.waitForCompletion(true)) {
                System.out.println("Job failed.");
                System.exit(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            final double duration = (System.currentTimeMillis() - startTime)/1000.0;
            System.out.println("Duration is " + duration + " seconds.");
        }
        return 0;
    }


    public static void main(String[] argv) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ThreelMain(), argv));
    }
}