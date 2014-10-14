package ca.uwaterloo.iss4e.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

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
public class CartesianRecordReader<K1, V1, K2, V2> extends RecordReader<Text, Text> {
    private RecordReader leftRR = null, rightRR = null;
    private FileInputFormat rightFIF;
    private InputSplit rightIS;
    private InputSplit leftIS;
    private TaskAttemptContext rightTaskAttemptContext;

    private Text key;
    private Text value;

    private boolean goToNextLeft = true;
    private boolean alldone = false;

    public CartesianRecordReader(CompositeInputSplit split, TaskAttemptContext taskAttemptContext) throws IOException {

        this.leftIS = split.get(0);
        this.rightIS = split.get(1);
        this.rightTaskAttemptContext = taskAttemptContext;
        this.key = new Text();
        this.value = new Text();
        Configuration conf = rightTaskAttemptContext.getConfiguration();
        try {
            // Create left record reader
            FileInputFormat leftFIF = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(conf
                    .get(CartesianInputFormat.LEFT_INPUT_FORMAT)), conf);

            leftRR = leftFIF.createRecordReader(leftIS, taskAttemptContext);

            // Create right record reader
            rightFIF = (FileInputFormat) ReflectionUtils.newInstance(Class
                            .forName(conf.get(CartesianInputFormat.RIGHT_INPUT_FORMAT)),conf);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IOException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        leftRR.initialize(this.leftIS, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        do {
            // If we are to go to the next left key/value pair
            if (goToNextLeft) {
                // Read the next key value pair, false means no more pairs
                if (!leftRR.nextKeyValue()) {
                    // If no more, then this task is nearly finished
                    alldone = true;
                    break;
                } else {
                    // If we aren't done, set the value to the key and set our flags
                    key.set(leftRR.getCurrentValue().toString());
                    goToNextLeft = alldone = false;

                    // Reset the right record reader
                    this.rightRR = this.rightFIF.createRecordReader(this.rightIS, this.rightTaskAttemptContext);
                    this.rightRR.initialize(this.rightIS, this.rightTaskAttemptContext);
                }
            }

            // Read the next key value pair from the right data set
            if (rightRR.nextKeyValue()) {
                // If success, set the value
                value.set(rightRR.getCurrentValue().toString());
            } else {
                // Otherwise, this right data set is complete
                // and we should go to the next left pair
                goToNextLeft = true;
            }

            // This loop will continue if we finished reading key/value
            // pairs from the right data set
        } while (goToNextLeft);

        // Return true if a key/value pair was read, false otherwise
        return !alldone;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return leftRR.getProgress();
    }

    @Override
    public void close() throws IOException {
        leftRR.close();
        rightRR.close();
    }
}
