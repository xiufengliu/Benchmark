package ca.uwaterloo.iss4e.algorithm;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.io.*;   // for IOException, PrintWriter, FileOutputStream
import java.text.*; // for DecimalFormat
import java.util.List;
/**
 *
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

public class Histogram implements Serializable {
    public static int[] calcHistogram(double[] data, double min, double max, int numBins) {
        final int[] result = new int[numBins];
        final double binSize = (max - min) / numBins;

        for (double d : data) {
            int bin = (int) ((d - min) / numBins);
            if (bin < 0) { /* this data is smaller than min */ } else if (bin >= numBins) { /* this data point is bigger than max */ } else {
                result[bin] += 1;
            }
        }
        return result;
    }

    public static int[] calcHistogram(List<Double> sortedList, int numBins) {
        final int[] result = new int[numBins];
        int size = sortedList.size();
        if (size > 0) {
            double min = sortedList.get(0);
            double max = sortedList.get(size - 1);
            final double binSize = (max - min) / numBins;

            for (double d : sortedList) {
                int bin = (int) ((d - min) / numBins);
                if (bin < 0) { /* this data is smaller than min */ } else if (bin >= numBins) { /* this data point is bigger than max */ } else {
                    result[bin] += 1;
                }
            }
        }
        return result;
    }

    public static int[] calcWritableHistogram(List<DoubleWritable> sortedList, int numBins) {
        final int[] result = new int[numBins];
        int size = sortedList.size();
        if (size > 0) {
            double min = sortedList.get(0).get();
            double max = sortedList.get(size - 1).get();
            final double binSize = (max - min) / numBins;

            for (DoubleWritable d : sortedList) {
                int bin = (int) ((d.get() - min) / numBins);
                if (bin < 0) { /* this data is smaller than min */ } else if (bin >= numBins) { /* this data point is bigger than max */ } else {
                    result[bin] += 1;
                }
            }
        }
        return result;
    }

}
