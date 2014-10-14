package ca.uwaterloo.iss4e.hive.meterperrow;

import ca.uwaterloo.iss4e.algorithm.Cosine;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

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
