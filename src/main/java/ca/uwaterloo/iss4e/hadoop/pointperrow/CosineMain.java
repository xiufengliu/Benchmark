package ca.uwaterloo.iss4e.hadoop.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Cosine;
import ca.uwaterloo.iss4e.hadoop.io.CartesianInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

public class CosineMain extends Configured implements Tool {
    public static final String DESCRIPTION = "Consine similarity program";

    private static final Log LOG = LogFactory.getLog(CosineMain.class);

    public static class AggregateReadingsMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, LongWritable, DoubleWritable> {
        @Override
        protected void map(Object obj, Text line, final Context context) throws IOException, InterruptedException {
            String[] values = line.toString().split(",");
            context.write(new LongWritable(Integer.parseInt(values[0])), new DoubleWritable(Double.parseDouble(values[2])));
        }
    }

    public static class AggregateReadingsReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable meterID, Iterable<DoubleWritable> values,
                              Context context) throws IOException, InterruptedException {
            StringBuffer buf = new StringBuffer();
            Iterator<DoubleWritable> itr = values.iterator();
            while (itr.hasNext()) {
                DoubleWritable value = itr.next();
                buf.append(value.get()).append(",");
            }
            context.write(meterID, new Text(buf.toString()));
        }
    }

    public static class DescendingKeyComparator extends WritableComparator{

        public  DescendingKeyComparator(){
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            return -1*a.compareTo(b);
        }
    }


    public static class CartesianProductMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, DoubleWritable, Text> {
        //DecimalFormat df4 = new DecimalFormat("###.######");

        @Override
        protected void map(Text left, Text right, final Context context) throws IOException, InterruptedException {
            String[] leftFieldValues = left.toString().split("\t");
            String[] rightFieldValues = right.toString().split("\t");
            int leftMeterID = Integer.parseInt(leftFieldValues[0]);
            int rightMeterID = Integer.parseInt(rightFieldValues[0]);
            if (leftMeterID != rightMeterID) {
                String[] leftReadings = leftFieldValues[1].split(",");
                String[] rightReadings = rightFieldValues[1].split(",");
                //String similarity = df4.format(Cosine.cosine_similarity(leftReadings, rightReadings));
                double similarity = Cosine.cosine_similarity(leftReadings, rightReadings);
                Text value = new Text();
                if (leftMeterID < rightMeterID) {
                    value.set(leftMeterID + "," + rightMeterID);
                } else {
                    value.set(rightMeterID + "," + leftMeterID);
                }
                context.write(new DoubleWritable(similarity), value);
            }
        }
    }

    public static class CartesianProductReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        Map<String, Double> results = new HashMap<String, Double>();

        @Override
        protected void reduce(DoubleWritable similarity, Iterable<Text> meterIDs,
                              Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = meterIDs.iterator();
            while (itr.hasNext()) {
                String meterIDPair = itr.next().toString();
                if (results.size()<10 && !results.containsKey(meterIDPair)){
                    results.put(meterIDPair, similarity.get());
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry:results.entrySet()){
                context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
            }
        }
    }


    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ca.uwaterloo.iss4e.hadoop.pointperrow.ConsineMain <input> <output>");
            System.exit(2);
        }
        Job job1 = new Job(conf, "ConsineMain");
        job1.setJarByClass(CosineMain.class);


        job1.setMapperClass(AggregateReadingsMapper.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setReducerClass(AggregateReadingsReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.setInputDirRecursive(job1, true);
        FileInputFormat.setInputPaths(job1, new Path(otherArgs[0]));
        int lastIdx = otherArgs[0].lastIndexOf("/");
        String tempOutput = otherArgs[0].substring(0, lastIdx) + "/temp";
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput));


        System.out.println("\nStarting Job-1 ...");
        final long startTime = System.currentTimeMillis();
        try {
            final long startTimeJob1 = System.currentTimeMillis();
            if (!job1.waitForCompletion(true)) {
                System.out.println("Job-1 failed.");
            } else {
                System.out.println("Duration of Job1 " + ((System.currentTimeMillis() - startTimeJob1) / 1000.0) + " seconds.");
                final Job job2 = new Job(conf, "ConsineMain Aggregate");
                job2.setJarByClass(CosineMain.class);
                job2.setInputFormatClass(CartesianInputFormat.class);
                CartesianInputFormat.setLeftInputInfo(job2, TextInputFormat.class, tempOutput);
                CartesianInputFormat.setRightInputInfo(job2, TextInputFormat.class, tempOutput);
                FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

                job2.setMapperClass(CartesianProductMapper.class);
                job2.setMapOutputKeyClass(DoubleWritable.class);
                job2.setMapOutputValueClass(Text.class);

                job2.setSortComparatorClass(DescendingKeyComparator.class);

                job2.setReducerClass(CartesianProductReducer.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(DoubleWritable.class);

                job2.setNumReduceTasks(10);
                final long startTimeJob2 = System.currentTimeMillis();
                System.out.println("\nStarting Job-2 ...");
                if (!job2.waitForCompletion(true)) {
                    System.out.println("Job-2 failed.");
                }else{
                    System.out.println("Duration of Job2: " + ((System.currentTimeMillis() - startTimeJob2) / 1000.0) + " seconds.");
                }

            }
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(tempOutput), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Total Duration: " + duration + " seconds.");
        }
        return 0;
    }

    public int run1(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: java " + getClass().getName() + " <inputDir> <outDir> <ntasks>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = getConf();
        final Job job2 = new Job(conf, "ConsineMain cartesian product");
        job2.setJarByClass(CosineMain.class);

        job2.setInputFormatClass(CartesianInputFormat.class);
        CartesianInputFormat.setLeftInputInfo(job2, TextInputFormat.class, args[0]);
        CartesianInputFormat.setRightInputInfo(job2, TextInputFormat.class, args[0]);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setMapperClass(CartesianProductMapper.class);
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setSortComparatorClass(DescendingKeyComparator.class);

        job2.setReducerClass(CartesianProductReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setNumReduceTasks(Integer.parseInt(args[2]));

        System.out.println("\nStarting Job-2 ...");
        final long startTime = System.currentTimeMillis();
        try {
            if (!job2.waitForCompletion(true)) {
                System.out.println("Job-2 failed.");
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
        System.exit(ToolRunner.run(new Configuration(), new CosineMain(), argv));
    }
}