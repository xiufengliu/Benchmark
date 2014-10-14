package ca.uwaterloo.iss4e.algorithm;

import java.io.Serializable;
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

public class Cosine implements Serializable {

    public static Double cosine_similarity(List<Double> vec1, List<Double> vec2) {

            int size = Math.min(vec1.size(), vec2.size());
            double dp = dot_product(vec1, vec2, size);
            double magnitudeA = find_magnitude(vec1, size);
            double magnitudeB = find_magnitude(vec2, size);
            return Double.valueOf((dp) / (magnitudeA * magnitudeB));

    }


    public static Double cosine_similarity(Double[] vec1, Double[] vec2) {
        int size = Math.min(vec1.length, vec2.length);
        double dp = dot_product(vec1, vec2, size);
        double magnitudeA = find_magnitude(vec1, size);
        double magnitudeB = find_magnitude(vec2, size);
        return Double.valueOf((dp) / (magnitudeA * magnitudeB));
    }

    public static Double cosine_similarity(String[] vec1, String[] vec2) {
        int size = Math.min(vec1.length, vec2.length);
        double dp = dot_product(vec1, vec2, size);
        double magnitudeA = find_magnitude(vec1, size);
        double magnitudeB = find_magnitude(vec2, size);
        return Double.valueOf((dp) / (magnitudeA * magnitudeB));
    }

    private static double find_magnitude(String[] vec, int size) {
        double sum_mag = 0;
        for (int i = 0; i < size; i++) {
            sum_mag = sum_mag + Double.parseDouble(vec[i]) * Double.parseDouble(vec[i]);
        }
        return Math.sqrt(sum_mag);
    }


    private static double find_magnitude(List<Double>  vec, int size) {
        double sum_mag = 0;
        for (int i = 0; i < size; i++) {
            sum_mag = sum_mag + vec.get(i) * vec.get(i);
        }
        return Math.sqrt(sum_mag);
    }

    private static double find_magnitude(Double[] vec, int size) {
        double sum_mag = 0;
        for (int i = 0; i < size; i++) {
            sum_mag = sum_mag + vec[i] * vec[i];
        }
        return Math.sqrt(sum_mag);
    }


    private static double dot_product(Double[] vec1, Double[] vec2, int size) {
        double sum = 0;
        for (int i = 0; i < size; i++) {
            sum = sum + vec1[i] * vec2[i];
        }
        return sum;
    }

    private static double dot_product(List<Double> vec1,List<Double> vec2, int size) {
        double sum = 0;
        for (int i = 0; i < size; i++) {
            sum = sum + vec1.get(i) * vec2.get(i);
        }
        return sum;
    }

    private static double dot_product(String[] vec1, String[] vec2, int size) {
        double sum = 0;
        for (int i = 0; i < size; i++) {
            sum = sum + Double.parseDouble(vec1[i]) * Double.parseDouble(vec2[i]);
        }
        return sum;
    }
}
