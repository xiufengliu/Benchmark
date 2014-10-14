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
 * Created by xiliu on 25/09/14.
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
