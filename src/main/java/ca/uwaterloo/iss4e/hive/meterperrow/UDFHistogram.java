package ca.uwaterloo.iss4e.hive.meterperrow;

import ca.uwaterloo.iss4e.algorithm.Histogram;
import ca.uwaterloo.iss4e.algorithm.Threelines;
import ca.uwaterloo.iss4e.common.ArrayListWritable;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

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
public class UDFHistogram extends UDF {
    public List<IntWritable> evaluate(List<Double> values) {
        if (values == null) {
            return null;
        }

        int[] hist = Histogram.calcHistogram(values, 10);
        if (values == null) return null;

        List<IntWritable> ret = new ArrayList<IntWritable>();
        for (int i = 0; i < hist.length; ++i) {
            ret.add(new IntWritable(hist[i]));
        }
        return ret;
    }
}
