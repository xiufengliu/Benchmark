package ca.uwaterloo.iss4e.hive.meterperrow;


import ca.uwaterloo.iss4e.algorithm.Threelines;
import ca.uwaterloo.iss4e.common.ArrayListWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a simple UDAF that concatenates all arguments from different rows
 * into a single string.
 * <p/>
 * It should be very easy to follow and can be used as an example for writing
 * new UDAFs.
 * <p/>
 * Note that Hive internally uses a different mechanism (called GenericUDAF) to
 * implement built-in aggregation functions, which are harder to program but
 * more efficient.
 */

public class UDFThreel extends UDF {

    public UDFThreel() {
    }

//collect_list
    public List<ArrayListWritable<DoubleWritable>> evaluate(List<Double>[] values) {
        if (values == null || values.length != 2) {
            return null;
        }
        List<Double> readings = values[0];
        List<Double> temperatures = values[1];
        double[][] results = Threelines.threel(temperatures, readings);
        if (results == null) {
            return null;
        }

        List<ArrayListWritable<DoubleWritable>> points = new ArrayList<ArrayListWritable<DoubleWritable>>();
        for (int i = 0; i < results.length; ++i) {
            ArrayListWritable<DoubleWritable> point = new ArrayListWritable<DoubleWritable>();
            double[] result = results[i];
            for (int j = 0; j < result.length; ++j) {
                point.add(new DoubleWritable(result[j]));
            }
            points.add(point);
        }
        return points;
    }

}