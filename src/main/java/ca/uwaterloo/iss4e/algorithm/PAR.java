package ca.uwaterloo.iss4e.algorithm;

import ca.uwaterloo.iss4e.common.ArrayListWritable;
import ca.uwaterloo.iss4e.common.SMASException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.io.Serializable;
import java.util.ArrayList;
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

public class PAR implements Serializable {


    public static double[][] makeSeries(final Double[] observations, int parts) {
        int size = observations.length / parts;
        double[][] series = new double[parts][size];
        for (int i = 0; i < parts; ++i) {
            int n = 0;
            for (int j = observations.length % parts + i; j < observations.length; j += parts) {
                series[i][n++] = observations[j].doubleValue();
            }
        }
        return series;
    }

    public static double[][] makeSeries2(final List<Double> observations, int parts) {
        int size = observations.size() / parts;
        double[][] series = new double[parts][size];
        for (int i = 0; i < parts; ++i) {
            int n = 0;
            for (int j = observations.size() % parts + i; j < observations.size(); j += parts) {
                series[i][n++] = observations.get(j).doubleValue();
            }
        }
        return series;
    }

    public static double[][] makeSeries(final List<DoubleWritable> observations, int parts) {
        int size = observations.size() / parts;
        double[][] series = new double[parts][size];
        for (int i = 0; i < parts; ++i) {
            int n = 0;
            for (int j = observations.size() % parts + i; j < observations.size(); j += parts) {
                series[i][n++] = observations.get(j).get();
            }
        }
        return series;
    }

    public static Pair makePair(final double[] serie, int order) {
        int size = serie.length - order;
        double[] Y = new double[size];
        double[][] X = new double[size][order];
        for (int i = 0; i < size; ++i) {
            for (int j = 0; j < order; ++j) {
                X[i][j] = serie[i + j];
            }
            Y[i] = serie[order + i];
        }
        return new Pair(Y, X);
    }

    public static Double[][] computeParameters(final Double[] readings, int order, int seasons) {
        Double[][] parameters = new Double[seasons][];
        try {
            double[][] series = makeSeries(readings, seasons);
            for (int season = 0; season < series.length; ++season) {
                double[] serie = series[season];
                Pair pair = makePair(serie, order);
                double[] Y = (double[]) pair.getKey();
                double[][] X = (double[][]) pair.getValue();
                OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
                // regression.setNoIntercept(true);
                regression.newSampleData(Y, X);
                double[] params = regression.estimateRegressionParameters();
                Double[] bigParams = new Double[params.length];
                for (int i = 0; i < params.length; ++i) {
                    bigParams[i] = Double.valueOf(params[i]);
                }
                parameters[season] = bigParams;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return parameters;
    }

    public static List<ArrayListWritable<DoubleWritable>> computeParameters(final List<DoubleWritable> readings, int order, int seasons) {
        List<ArrayListWritable<DoubleWritable>> parameters = new ArrayList<ArrayListWritable<DoubleWritable>>();
        try {
            double[][] series = makeSeries(readings, seasons);
            for (int season = 0; season < series.length; ++season) {
                double[] serie = series[season];
                Pair pair = makePair(serie, order);
                double[] Y = (double[]) pair.getKey();
                double[][] X = (double[][]) pair.getValue();
                OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
                // regression.setNoIntercept(true);
                regression.newSampleData(Y, X);
                double[] params = regression.estimateRegressionParameters();
                //Double[] bigParams = new Double[params.length];
                ArrayListWritable<DoubleWritable> bigParams = new  ArrayListWritable<DoubleWritable>();
                for (int i = 0; i < params.length; ++i) {
                    bigParams.add(new DoubleWritable(params[i]));
                }
                parameters.add(bigParams);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return parameters;
    }

    public static List<ArrayListWritable<DoubleWritable>> computeParameters2(final List<Double> readings, int order, int seasons) {
        List<ArrayListWritable<DoubleWritable>> parameters = new ArrayList<ArrayListWritable<DoubleWritable>>();
        try {
            double[][] series = makeSeries2(readings, seasons);
            for (int season = 0; season < series.length; ++season) {
                double[] serie = series[season];
                Pair pair = makePair(serie, order);
                double[] Y = (double[]) pair.getKey();
                double[][] X = (double[][]) pair.getValue();
                OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
                // regression.setNoIntercept(true);
                regression.newSampleData(Y, X);
                double[] params = regression.estimateRegressionParameters();
                //Double[] bigParams = new Double[params.length];
                ArrayListWritable<DoubleWritable> bigParams = new  ArrayListWritable<DoubleWritable>();
                for (int i = 0; i < params.length; ++i) {
                    bigParams.add(new DoubleWritable(params[i]));
                }
                parameters.add(bigParams);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return parameters;
    }
}
