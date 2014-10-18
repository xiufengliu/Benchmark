package ca.uwaterloo.iss4e.hive.meterperfile;

import ca.uwaterloo.iss4e.algorithm.Histogram;
import ca.uwaterloo.iss4e.algorithm.PAR;
import ca.uwaterloo.iss4e.algorithm.Threelines;
import ca.uwaterloo.iss4e.common.ArrayListWritable;
import com.sun.corba.se.impl.encoding.OSFCodeSetRegistry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

@Description(name = "hist", value = "_FUNC_(meterid, reading) - emits () for the histogram programs")
public class UDTFHistogram extends GenericUDTF {

    private PrimitiveObjectInspector doubleOI = null;
    private PrimitiveObjectInspector intOI = null;
    private List<Double> readings = new ArrayList<Double>();
    private Map<Integer, int[]> resultMap = new HashMap<Integer, int[]>();
    private Object[] forwardObj = new Object[2];
    private int preMeterID = -1;




    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("hist() takes exactly two arguments, meterid and reading ");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT &&
                args[1].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE
                ) {
            throw new UDFArgumentException("hist() takes double as a parameter");
        }

        intOI = (PrimitiveObjectInspector) args[0];
        doubleOI = (PrimitiveObjectInspector) args[1];
        //StandardListObjectInspector outputOI0 = (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
        //StandardListObjectInspector outputOI1 = (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(outputOI0);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        //fieldNames.add("meterid");
        fieldNames.add("result");

        // fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT));
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        final Integer curMeterID = (Integer) intOI.getPrimitiveJavaObject(record[0]);
        if (preMeterID != -1 && curMeterID.intValue() != preMeterID) {
            int[] params = Histogram.calcHistogram(readings, 10);
            if (params != null)
                resultMap.put(preMeterID, params);
            readings.clear();
        }
        final Double reading = (Double) doubleOI.getPrimitiveJavaObject(record[1]);
        if (reading == null) {
            return;
        }
        preMeterID = curMeterID.intValue();
        readings.add(reading.doubleValue());
    }

    @Override
    public void close() throws HiveException {
        if (readings.size() > 0) {
            int[] params = Histogram.calcHistogram(readings, 10);
            if (params != null)
                resultMap.put(preMeterID, params);
        }
        StringBuffer buf = new StringBuffer();
        for (Map.Entry<Integer, int[]> entry : resultMap.entrySet()) {
            buf.setLength(0);
            buf.append(entry.getKey().intValue()).append(",");
            int[] parameters = entry.getValue();
            buf.append("[");
            for (int i = 0; i < parameters.length; ++i) {
                buf.append(parameters[i]);
                if (i < parameters.length - 1)
                    buf.append(",");
            }
            buf.append("]");
            forwardObj[0] = buf.toString();
            forward(forwardObj);
        }
    }
}