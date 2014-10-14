package ca.uwaterloo.iss4e.hadoop.meterperfile;

import ca.uwaterloo.iss4e.algorithm.Threelines;
import ca.uwaterloo.iss4e.hadoop.io.UnsplitableTextInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.*;

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

public class ThreelMain extends Configured implements Tool {
    public static final String DESCRIPTION = "Threeline regression program";

    private static final Log LOG = LogFactory.getLog(ThreelMain.class);

    public static class MyMapper extends Mapper<Object, Text, LongWritable, Text> {

        List<Double> readings = new ArrayList<Double>();
        List<Double> temperatures = new ArrayList<Double>();
        private int preMeterID = -1;
        private Map<Integer, double[][]> resultMap = new HashMap<Integer, double[][]>();
        StringBuffer ret = new StringBuffer();

        @Override
        protected void map(Object offset, Text line, final Context context) throws IOException, InterruptedException {
            String[] fieldValues = line.toString().split(",");
            int curMeterID = Integer.parseInt(fieldValues[0]);
            if (preMeterID != -1 && preMeterID != curMeterID) {
                computerAndWriteOutput(context);
            }
            preMeterID = curMeterID;
            readings.add(Double.parseDouble(fieldValues[2]));
            temperatures.add(Double.parseDouble(fieldValues[3]));
        }

        @Override
        protected void cleanup(Context context) throws java.io.IOException, java.lang.InterruptedException {
            computerAndWriteOutput(context);
        }

        protected void computerAndWriteOutput(Context context) throws IOException, InterruptedException {
            if (temperatures.size() > 0 && readings.size() > 0) {
                double[][] points = Threelines.threel(temperatures, readings);
                if (points != null) {
                    ret.setLength(0);
                    ret.append("[");
                    for (int i = 0; i < points.length; ++i) {
                        double[] point = points[i];
                        ret.append("[");
                        ret.append(point[0]).append(",").append(point[1]);
                        ret.append("]");
                        if (i < points.length - 1)
                            ret.append(",");
                    }
                    ret.append("]");
                    context.write(new LongWritable(preMeterID), new Text(ret.toString()));
                }
                temperatures.clear();
                readings.clear();
            }
        }
    }

    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.hadoop.meterperfile.ThreelMain <input> <output>");
            System.exit(2);
        }

        conf.set("mapreduce.input.fileinputformat.split.maxsize", "100");
        Job job = new Job(conf, "ThreelMain");
        job.setJarByClass(ThreelMain.class);

        job.setInputFormatClass(UnsplitableTextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        // job.setOutputKeyClass(LongWritable.class);
        //job.setOutputValueClass(Text.class);
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
        System.exit(ToolRunner.run(new Configuration(), new ThreelMain(), argv));
    }
}