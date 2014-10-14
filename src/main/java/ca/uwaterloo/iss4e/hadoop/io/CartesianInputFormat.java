package ca.uwaterloo.iss4e.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
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

// https://github.com/adamjshook/mapreducepatterns/blob/master/MRDP/src/main/java/mrdp/ch5/CartesianProduct.java
public class CartesianInputFormat extends FileInputFormat {
    private static final Log LOG = LogFactory.getLog(CartesianInputFormat.class);

    public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
    public static final String LEFT_INPUT_PATH = "cart.left.path";
    public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
    public static final String RIGHT_INPUT_PATH = "cart.right.path";



    public static void setLeftInputInfo(Job job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
        Configuration conf = job.getConfiguration();
        conf.set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
        conf.set(LEFT_INPUT_PATH, inputPath);
    }

    public static void setRightInputInfo(Job job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
        Configuration conf = job.getConfiguration();
        conf.set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
        conf.set(RIGHT_INPUT_PATH, inputPath);
    }


    private List<InputSplit> getInputSplits(JobContext jobContext,
                                            String inputFormatClass, Path path)
            throws ClassNotFoundException, IOException {
        Configuration conf = jobContext.getConfiguration();
        FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(inputFormatClass), conf);

        // Set the input path for the left data set
        path = path.getFileSystem(conf).makeQualified(path);
        String dirStr = StringUtils.escapeString(path.toString());
        String dirs = conf.get(INPUT_DIR);
        conf.set(INPUT_DIR, dirStr);
         return inputFormat.getSplits(jobContext);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        try {
            // Get the input splits from both the left and right data sets
            Configuration conf = job.getConfiguration();
            List<InputSplit> leftSplits = getInputSplits(job, conf.get(LEFT_INPUT_FORMAT), new Path(conf.get(LEFT_INPUT_PATH)));
            List<InputSplit> rightSplits = getInputSplits(job, conf.get(RIGHT_INPUT_FORMAT), new Path(conf.get(RIGHT_INPUT_PATH)));

            // Create our CompositeInputSplits, size equal to left.length *
            // right.length
            List<InputSplit> compoisteInputSplits = new ArrayList<InputSplit>();


            // For each of the left input splits
            for (InputSplit left : leftSplits) {
                // For each of the right input splits
                for (InputSplit right : rightSplits) {
                    // Create a new composite input split composing of the
                    // two
                    CompositeInputSplit returnSplits = new CompositeInputSplit(2);
                    returnSplits.add(left);
                    returnSplits.add(right);
                    compoisteInputSplits.add(returnSplits);
                }
            }

            // Return the composite splits
             LOG.info("Total CompositeSplits to process: " + compoisteInputSplits.size());
            return compoisteInputSplits;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IOException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public RecordReader createRecordReader(org.apache.hadoop.mapreduce.InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CartesianRecordReader((CompositeInputSplit) inputSplit, taskAttemptContext);
    }
}