package ca.uwaterloo.iss4e.hive.meterperrow;

import ca.uwaterloo.iss4e.algorithm.Cosine;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiliu on 25/09/14.
 */
public class UDFColAsc extends UDF {
    List<IntWritable> ret = new ArrayList<IntWritable>();

    public UDFColAsc(){
        ret.add(new IntWritable());
        ret.add(new IntWritable());
    }
    public List<IntWritable> evaluate(Integer[] values) {
        if (values == null || values.length != 2) {
            return null;
        }
        if (values[0].intValue() < values[1].intValue()) {
            ret.get(0).set(values[0].intValue());
            ret.get(1).set(values[1].intValue());
        } else {
            ret.get(0).set(values[1].intValue());
            ret.get(1).set(values[0].intValue());
        }
        return ret;
    }
}
