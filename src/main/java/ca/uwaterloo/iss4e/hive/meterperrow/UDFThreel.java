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