package ca.uwaterloo.iss4e.hive.meterperfile;

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


@Description(name = "threel", value = "_FUNC_(meterid, reading, temperature) - emits () for the four points of the threel programs")
public class UDTFThreel extends GenericUDTF {

    private PrimitiveObjectInspector doubleOI = null;
    private PrimitiveObjectInspector intOI = null;
    private List<Double> readings = new ArrayList<Double>();
    private List<Double> temperatures = new ArrayList<Double>();
    private Map<Integer, double[][]> resultMap = new HashMap<Integer, double[][]>();
    private Object[] forwardObj = new Object[2];
    private int preMeterID = -1;


    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 3) {
            throw new UDFArgumentException("threel() takes exactly three arguments, meterid, reading and temperature");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT &&
                args[1].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE &&
                args[2].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[2]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE
                ) {
            throw new UDFArgumentException("threel() takes double as a parameter");
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
        if (preMeterID!=-1 && curMeterID.intValue()!=preMeterID){
            double[][] points = Threelines.threel(temperatures, readings);
            if (points!=null)
                resultMap.put(preMeterID,points);
            temperatures.clear();
            readings.clear();
        }
        final Double reading = (Double) doubleOI.getPrimitiveJavaObject(record[1]);
        final Double temperature = (Double) doubleOI.getPrimitiveJavaObject(record[2]);
        if (reading == null && temperature == null) {
            return;
        }
        preMeterID = curMeterID.intValue();
        readings.add(reading.doubleValue());
        temperatures.add(temperature.doubleValue());
    }

    @Override
    public void close() throws HiveException {

        if (temperatures.size()>0 && readings.size()>0){
            double[][] points = Threelines.threel(temperatures, readings);
            if (points!=null)
                resultMap.put(preMeterID,points);
        }
        StringBuffer buf = new StringBuffer();
        for (Map.Entry<Integer, double[][]> entry: resultMap.entrySet()){
            buf.setLength(0);
            buf.append(entry.getKey().intValue()).append(",");
            double[][] points = entry.getValue();
            buf.append("[");
            for (int i=0; i<points.length; ++i){
                double[] point = points[i];
                buf.append("[");
                buf.append(point[0]).append(",").append(point[1]);
                buf.append("]");
                if (i<points.length-1)
                    buf.append(",");
            }
            buf.append("]");
            forwardObj[0] = buf.toString();
            forward(forwardObj);
        }
    }
}