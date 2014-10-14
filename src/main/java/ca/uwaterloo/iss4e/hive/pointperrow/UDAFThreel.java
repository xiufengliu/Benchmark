package ca.uwaterloo.iss4e.hive.pointperrow;

import ca.uwaterloo.iss4e.algorithm.Threelines;
import ca.uwaterloo.iss4e.common.ArrayListWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
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
@Description(name = "UDAFThreel",
        value = "_FUNC_(reading, temperature) - ")
public class UDAFThreel extends UDAF {
    static final Log LOG = LogFactory.getLog(UDAFThreel.class.getName());

    public static class State {
        private List<DoubleWritable> readings;
        private List<DoubleWritable> temperatures;
    }

    public static class UDAFThreelEvaluator implements UDAFEvaluator {


        private final State state;

        public UDAFThreelEvaluator() {
            state = new State();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
            if (state.readings != null) {
                state.readings.clear();
            }
            if (state.temperatures != null) {
                state.temperatures.clear();
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
        public boolean iterate(Double[] objs) {
            if (objs != null) {
                if (objs.length != 2) {
                    return false;
                }
                if (objs[0] != null) {
                    if (state.readings == null) {
                        state.readings = new ArrayList<DoubleWritable>();
                    }
                    state.readings.add(new DoubleWritable(objs[0]));
                }
                if (objs[1] != null) {
                    if (state.temperatures == null) {
                        state.temperatures = new ArrayList<DoubleWritable>();
                    }
                    state.temperatures.add(new DoubleWritable(objs[1]));
                }
            }
            return true;
        }

        /**
         * Terminate a partial aggregation and return the state.
         */
        public State terminatePartial() {
            return state;
        }

        /**
         * Merge with a partial aggregation.
         * <p/>
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         * <p/>
         * This function should always return true.
         */
        public boolean merge(State other) {
            if (other == null || other.readings == null || other.temperatures == null) {
                return true;
            }

            if (state.readings == null) {
                state.readings = new ArrayList<DoubleWritable>();
            }
            state.readings.addAll(other.readings);

            if (state.temperatures == null) {
                state.temperatures = new ArrayList<DoubleWritable>();
            }
            state.temperatures.addAll(other.temperatures);

            return true;
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public List<ArrayListWritable<DoubleWritable>> terminate() {
            if (state != null && state.readings != null && state.temperatures != null) {
                double[][] results = Threelines.threelWritable(state.temperatures, state.readings);
                List<ArrayListWritable<DoubleWritable>> points = new ArrayList<ArrayListWritable<DoubleWritable>>();
                //StringBuffer buf = new StringBuffer();
                for (int i = 0; i < results.length; ++i) {
                    ArrayListWritable<DoubleWritable> point = new ArrayListWritable<DoubleWritable>();
                    double[] result = results[i];
                    for (int j = 0; j < result.length; ++j) {
                        point.add(new DoubleWritable(result[j]));
                      //  buf.append(result[j]).append(",");
                    }
                    points.add(point);
                }
                return points;
               // return new Text(buf.toString());
            } else {
                return null;
            }
        }
    }
}