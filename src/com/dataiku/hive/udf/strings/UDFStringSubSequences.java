package com.dataiku.hive.udf.strings;

import com.dataiku.hive.udf.arrays.UDFArraySubSequences;
import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;


@Description(name="string_subsequence", value="array<string> _FUNC_(string, sep, N, MAX) - split a string according to sep, generate all subsequences of size <= N up to MAX elements, and generate all corresponing strings joined by sep")
public class UDFStringSubSequences extends GenericUDTF {

    private ObjectInspectorConverters.Converter[] converters;

    StringObjectInspector stringOI;
    WritableConstantIntObjectInspector intOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length < 4) {
            throw new IllegalArgumentException("Missing parameters, 4 needed");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[1] = ObjectInspectorConverters.getConverter(arguments[1],PrimitiveObjectInspectorFactory.writableStringObjectInspector) ;
        converters[2] = ObjectInspectorConverters.getConverter(arguments[2],PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        converters[3] = ObjectInspectorConverters.getConverter(arguments[3],PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private final Object[] forwardObj = new Object[1];


    protected ArrayList<String> buffer = new ArrayList<String>();
    protected Joiner joiner = Joiner.on(" ");
    protected int count = 0;
    protected int max;


    protected void forwardBuffer() throws HiveException {
        Text t = new Text(joiner.join(buffer));
        forwardObj[0] = t;
        forward(forwardObj);
        count ++;
    }

    protected void enumerateSubSequenceStartAt(int start, int n, String[] elts) throws HiveException {
        if (count >= max) {
            return;
        }
        buffer.add(elts[start]);
        forwardBuffer();
        if (buffer.size() < n) {
            for(int j = start+1; j < n && j < elts.length; j++) {
               enumerateSubSequenceStartAt(j, n, elts);
            }
        }
        buffer.remove(buffer.size()-1);
    }


    protected void enumerateSubSequence(int n, String[] elts) throws HiveException {
        for(int i = 0; i < elts.length; i++) {
            enumerateSubSequenceStartAt(i, n, elts);
        }
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (objects.length < 4) {
            return;
        }
        count = 0;
        buffer.clear();
        Text text = (Text) converters[0].convert(objects[0]);
        Text sep = (Text) converters[1].convert(objects[1]);
        IntWritable n = (IntWritable) converters[2].convert(objects[2]);
        IntWritable max = (IntWritable) converters[3].convert(objects[3]);
        this.max = max.get();
        enumerateSubSequence(n.get(), text.toString().split(sep.toString()));
    }

    @Override
    public void close() throws HiveException {
    }
}
