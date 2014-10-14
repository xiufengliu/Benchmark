package ca.uwaterloo.iss4e.hive.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Histogram;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
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


@Description(name = "UDAFCollectArray", value = "_FUNC_(reading) - ")
public class UDAFCollectArray extends UDAF {
    static final Log LOG = LogFactory.getLog(UDAFCollectArray.class.getName());


    public static class UDAFCollectArrayEvaluator implements UDAFEvaluator {

        private List<DoubleWritable> readings;

        public UDAFCollectArrayEvaluator() {
        }

        /**
         * Reset the this of the aggregation.
         */
        public void init() {
            if (this.readings != null) {
                this.readings.clear();
            }
        }

        /**
         * Iterate through one row of original readings.
         * <p/>
         * This UDF accepts arbitrary number of String arguments, so we use
         * String[]. If it only accepts a single String, then we should use a single
         * String argument.
         * <p/>
         * This function should always return true.
         */
        public boolean iterate(Double objs) {
            if (objs != null) {
                if (this.readings == null) {
                    this.readings = new ArrayList<DoubleWritable>();
                }
                this.readings.add(new DoubleWritable(objs));

            }
            return true;
        }

        /**
         * Terminate a partial aggregation and return the this.
         */
        public List<DoubleWritable> terminatePartial() {
            return this.readings;
        }

        /**
         * Merge with a partial aggregation.
         * <p/>
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         * <p/>
         * This function should always return true.
         */
        public boolean merge(List<DoubleWritable> other) {
            if (other == null) {
                return true;
            }
            if (this.readings == null) {
                this.readings = new ArrayList<DoubleWritable>();
            }
            this.readings.addAll(other);

            return true;
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public Text terminate() {
            if (this.readings != null && this.readings.size() > 0) {
               StringBuffer buf = new StringBuffer();
                for (int i = 0; i < readings.size(); ++i) {
                     buf.append(readings.get(i));
                    if (i<readings.size()-1){
                        buf.append(";");
                    }
                }
               return new Text(buf.toString());
            } else {
                return null;
            }
        }
    }
}