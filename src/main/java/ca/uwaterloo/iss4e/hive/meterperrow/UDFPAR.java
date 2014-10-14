package ca.uwaterloo.iss4e.hive.meterperrow;

import ca.uwaterloo.iss4e.algorithm.PAR;
import ca.uwaterloo.iss4e.common.ArrayListWritable;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.util.List;

/**
 * Created by xiliu on 25/09/14.
 */
public class UDFPAR extends UDF {

    public List<ArrayListWritable<DoubleWritable>> evaluate(List<Double> values) {
        if (values == null) {
            return null;
        }
        return PAR.computeParameters2(values, 3, 24);

    }
}
