package ca.uwaterloo.iss4e.hive.meterperrow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

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
@Description(name = "cosine", value = "_FUNC_(doc) - emits similarity")
public class UDFCosine extends GenericUDF {
    static final Log LOG = LogFactory.getLog(UDFCosine.class.getName());
    private transient ObjectInspector[] listOIs;
    private transient PrimitiveObjectInspector listElemOI;
    private DoubleWritable result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 2) {
            throw new UDFArgumentTypeException(arguments.length - 1,
                    "Please specify two arguments.");
        }


        for (int i=0; i<arguments.length; ++i) {
            switch (arguments[i].getCategory()) {
                case LIST:
                    if (((ListObjectInspector) (arguments[i])).getListElementObjectInspector()
                            .getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
                        break;
                    }
                default:
                    throw new UDFArgumentTypeException(0, "Argument " + i
                            + " of function cosine must be " + serdeConstants.LIST_TYPE_NAME
                            + "<" + ObjectInspector.Category.PRIMITIVE + ">, but " + arguments[i].getTypeName()
                            + " was found.");
            }
        }

        listOIs = arguments;
        listElemOI = (PrimitiveObjectInspector) ((ListObjectInspector) (arguments[0])).getListElementObjectInspector();


        result = new DoubleWritable(0);
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);
        if (arguments[0] == null || arguments[1] == null) {
            return null;
        }
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }
        ListObjectInspector arrayOI0 = (ListObjectInspector) listOIs[0];
        List reading0 = (List) arrayOI0.getList(arguments[0].get());

        ListObjectInspector arrayOI1 = (ListObjectInspector) listOIs[1];
        List reading1 = (List) arrayOI1.getList(arguments[1].get());

        int size = Math.min(reading0.size(), reading1.size());
        if (size > 0) {
            double dotSum = 0.0;
            double magA = 0.0;
            double magB = 0.0;
            for (int i = 0; i < size; ++i) {
                double r0 = PrimitiveObjectInspectorUtils.getDouble(reading0.get(i), listElemOI);
                double r1 = PrimitiveObjectInspectorUtils.getDouble(reading1.get(i), listElemOI);
                dotSum += r0 * r1;
                magA += r0 * r0;
                magB += r1 * r1;
            }
            double similarity = dotSum / (Math.sqrt(magA) * Math.sqrt(magB));
            result.set(similarity);
            return result;
        } else {
            return null;
        }

        /*
        for(int i = 0; i < size; i++) {
            double reading1 = PrimitiveObjectInspectorUtils.getDouble(sloi1.getListElement(deferredObjects[0], i), (PrimitiveObjectInspector) sloi1.getListElementObjectInspector());
        }*/
        // return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length > 0);

        StringBuilder sb = new StringBuilder();
        sb.append("UDFCosine(");
        sb.append(children[0]);
        sb.append(")");

        return sb.toString();
    }


}