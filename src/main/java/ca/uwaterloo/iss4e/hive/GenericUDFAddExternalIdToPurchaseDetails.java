package ca.uwaterloo.iss4e.hive;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;

import java.util.ArrayList;

public class GenericUDFAddExternalIdToPurchaseDetails extends GenericUDF {

    // the return variable. Java Object[] become hive struct<>. Java ArrayList<> become hive array<>.
    // The return variable only holds the values that are in the struct<>. The field names
    // are defined in the ObjectInspector that is returned by the initialize() method.
    private ArrayList ret;

    // Global variables that inspect the input.
    // These are set up during the initialize() call, and are then used during the
    // calls to evaluate()
    //
    // ObjectInspector for the list (input array<>)
    // ObjectInspector for the struct<>
    // ObjectInspectors for the elements of the struct<>, target, quantity and price
    private ListObjectInspector loi;
    private StructObjectInspector soi;
    private ObjectInspector toi, qoi, poi;

    @Override
    // This is what we do in the initialize() method:
    // Verify that the input is of the type expected
    // Set up the ObjectInspectors for the input in global variables
    // Initialize the output ArrayList
    // Return the ObjectInspector for the output
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        // Verify the input is of the required type.
        // Set the global variables (the various ObjectInspectors) while we're doing this

        // Exactly one input argument
        if( arguments.length != 1 )
            throw new UDFArgumentLengthException("AddExternalIdToPurchaseDetails() accepts exactly one argument.");

        // Is the input an array<>
        if( arguments[0].getCategory() != ObjectInspector.Category.LIST )
            throw new UDFArgumentTypeException(0,"The single argument to AddExternalIdToPurchaseDetails should be "
                    + "Array<Struct>"
                    + " but " + arguments[0].getTypeName() + " is found");

        // Is the input an array<struct<>>
        // Get the object inspector for the list(array) elements; this should be a StructObjectInspector
        // Then check that the struct has the correct fields.
        // Also, store the ObjectInspectors for use later in the evaluate() method
        loi = ((ListObjectInspector)arguments[0]);
        soi = ((StructObjectInspector)loi.getListElementObjectInspector());

        // Are there the correct number of fields?
        if( soi.getAllStructFieldRefs().size() != 3 )
            throw new UDFArgumentTypeException(0,"Incorrect number of fields in the struct. "
                    + "The single argument to AddExternalIdToPurchaseDetails should be "
                    + "Array<Struct>"
                    + " but " + arguments[0].getTypeName() + " is found");

        // Are the fields the ones we want?
        StructField target = soi.getStructFieldRef("target");
        StructField quantity = soi.getStructFieldRef("quantity");
        StructField price = soi.getStructFieldRef("price");

        if( target==null )
            throw new UDFArgumentTypeException(0,"No \"target\" field in input structure "+arguments[0].getTypeName());
        if( quantity==null )
            throw new UDFArgumentTypeException(0,"No \"quantity\" field in input structure "+arguments[0].getTypeName());
        if( price==null )
            throw new UDFArgumentTypeException(0,"No \"price\" field in input structure "+arguments[0].getTypeName());

        // Are they of the correct types? (primitives WritableLong, WritableInt, WritableFloat)
        // We store these Object Inspectors for use in the evaluate() method.
        toi = target.getFieldObjectInspector();
        qoi = quantity.getFieldObjectInspector();
        poi = price.getFieldObjectInspector();

        // First, are they primitives?
        if(toi.getCategory() != ObjectInspector.Category.PRIMITIVE )
            throw new UDFArgumentTypeException(0,"Is input primitive? target field must be a bigint; found "+toi.getTypeName());
        if(qoi.getCategory() != ObjectInspector.Category.PRIMITIVE )
            throw new UDFArgumentTypeException(0,"Is input primitive? quantity field must be an int; found "+toi.getTypeName());
        if(poi.getCategory() != ObjectInspector.Category.PRIMITIVE )
            throw new UDFArgumentTypeException(0,"Is input primitive? price field must be a float; found "+toi.getTypeName());

        // Second, are they the correct type of primitive?
        if( ((PrimitiveObjectInspector)toi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG )
            throw new UDFArgumentTypeException(0,"Is input correct primitive? target field must be a bigint; found "+toi.getTypeName());
        if( ((PrimitiveObjectInspector)qoi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT )
            throw new UDFArgumentTypeException(0,"Is input correct primitive? target field must be an int; found "+toi.getTypeName());
        if( ((PrimitiveObjectInspector)poi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.FLOAT )
            throw new UDFArgumentTypeException(0,"Is input correct primitive? price field must be a float; found "+toi.getTypeName());

        // If we get to here, the input is an array<struct>

        // HOW TO RETURN THE OUTPUT?
        // A struct<> is stored as an Object[], with the elements being ,,...
        // See GenericUDFNamedStruct
        // The object inspector that we set up below and return at the end of initialize() takes care of the names,
        // so the Object[] only holds the values.
        // A java ArrayList is converted to a hive array<>, so the output is an ArrayList
        ret = new ArrayList();

        // Now set up and return the object inspector for the output of the UDF

        // Define the field names for the struct<> and their types
        ArrayList structFieldNames = new ArrayList();
        ArrayList structFieldObjectInspectors = new ArrayList();

        structFieldNames.add("target");
        structFieldNames.add("quantity");
        structFieldNames.add("price");
        structFieldNames.add("externalId");

        // To get instances of PrimitiveObjectInspector, we use the PrimitiveObjectInspectorFactory
        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableLongObjectInspector );
        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableIntObjectInspector );
        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableFloatObjectInspector );
        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );

        // Set up the object inspector for the struct<> for the output
        StructObjectInspector si2;
        si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);

        // Set up the list object inspector for the output, and return it
        ListObjectInspector li2;
        li2 = ObjectInspectorFactory.getStandardListObjectInspector( si2 );
        return li2;
    }

    @Override
    // The evaluate() method. The input is passed in as an array of DeferredObjects, so that
    // computation is not wasted on deserializing them if they're not actually used
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // Empty the return array (re-used between calls)
        ret.clear();

        // Should be exactly one argument
        if( arguments.length!=1 )
            return null;

        // If passed a null, return a null
        if( arguments[0].get()==null )
            return null;

        // Iterate over the elements of the input array
        // Convert the struct<>'s to the new format
        // Put them into the output array
        // Return the output array

        int nelements = loi.getListLength(arguments[0].get());
        for( int i=0; i<nelements; ++i){

        // getStructFieldData() returns an Object; however, it's actually a LazyLong
        // (How do we know it's a LazyLong, as the documentation says that getStructFieldData() returns an Object?
        //We know, because during development, the error in the hadoop task log was "can't cast LazyLong to ...")
        // How do you get the data out of a LazyLong? Using a LongObjectInspector...
        LazyLong LLtarget = (LazyLong)(soi.getStructFieldData( loi.getListElement(arguments[0].get(),i), soi.getStructFieldRef("target")));
        long tt = ((LongObjectInspector)toi).get( LLtarget );

        LazyInteger LIquantity = (LazyInteger)(soi.getStructFieldData( loi.getListElement(arguments[0].get(),i), soi.getStructFieldRef("quantity")));
        int qq = ((IntObjectInspector)qoi).get( LIquantity );

        LazyFloat LFprice = (LazyFloat)(soi.getStructFieldData( loi.getListElement(arguments[0].get(),i), soi.getStructFieldRef("price")));
        float pp = ((FloatObjectInspector)poi).get( LFprice );

        // The struct<> we're returning is stored as an Object[] of length 4 (it has 4 fields)
        Object[] e;
        e = new Object[4];

        // The field values must be inserted in the same order as defined in the ObjectInspector for the output
        // The fields must also be hadoop writable/text classes
        e[0] = new LongWritable(tt);
        e[1] = new IntWritable(qq);
        e[2] = new FloatWritable(pp);
        e[3] = new Text();

        ret.add(e);

    }

    return ret;
}

    @Override
    public String getDisplayString(String[] children) {
        assert( children.length>0 );

        StringBuilder sb = new StringBuilder();
        sb.append("AddExternalIdToPurchaseDetails(");
        sb.append(children[0]);
        sb.append(")");

        return sb.toString();
    }
}