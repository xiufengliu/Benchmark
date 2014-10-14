package ca.uwaterloo.iss4e.hadoop.pointperrow;

import ca.uwaterloo.iss4e.algorithm.PAR;
import ca.uwaterloo.iss4e.common.ArrayListWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

public class PARMain extends Configured implements Tool {
    public static final String DESCRIPTION = "Threeline regression program";

    private static final Log LOG = LogFactory.getLog(PARMain.class);

    public static class MyMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, LongWritable, DoubleWritable> {

        @Override
        protected void map(Object offset, Text line, final Context context) throws IOException, InterruptedException {
            String[] fieldValues = line.toString().split(",");
            context.write(new LongWritable(Long.valueOf(fieldValues[0])), new DoubleWritable(Double.parseDouble(fieldValues[2]))); // reading, temperature
        }
    }

    public static class MyCombiner extends Reducer<LongWritable, DoubleWritable, LongWritable, ArrayListWritable<DoubleWritable>> {

        @Override
        protected void reduce(LongWritable meterID, Iterable<DoubleWritable> values,
                              Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> itr = values.iterator();
            ArrayListWritable<DoubleWritable> readingWritables = new ArrayListWritable<DoubleWritable>();
            while (itr.hasNext()) {
                DoubleWritable value = itr.next();
                readingWritables.add(value);
            }
            context.write(meterID, readingWritables);
        }
    }

    public static class MyReducer extends Reducer<LongWritable, ArrayListWritable<DoubleWritable>, LongWritable, Text> {
        public static int ORDER = 3;
        public static int NUM_OF_SEASONS = 24;

        @Override
        protected void reduce(LongWritable meterID, Iterable<ArrayListWritable<DoubleWritable>> values,
                              Context context) throws IOException, InterruptedException {
            List<Double> readings = new ArrayList<Double>();
            Iterator<ArrayListWritable<DoubleWritable>> itr = values.iterator();
            while (itr.hasNext()) {
                ArrayListWritable<DoubleWritable> readingWritables = itr.next();
                for (int i = 0; i < readingWritables.size(); ++i) {
                    readings.add(readingWritables.get(i).get());
                }
            }
            Double[] readingArr = new Double[readings.size()];
            readingArr = readings.toArray(readingArr);
            Double[][] results = PAR.computeParameters(readingArr, MyReducer.ORDER, MyReducer.NUM_OF_SEASONS);
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < results.length; ++i) {
                Double[] pars = results[i];
                for (Double p : pars) {
                    buf.append(p.doubleValue()).append(",");
                }
            }
            context.write(meterID, new Text(buf.toString()));
        }
    }


    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.hadoop.pointperrow.PARMain <input> <output>");
            System.exit(2);
        }
        Job job = new Job(conf, "PARMain");
        job.setJarByClass(PARMain.class);


        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

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
            final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Duration is " + duration + " seconds.");
        }
        return 0;
    }

    public static void main(String[] argv) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PARMain(), argv));
    }
}