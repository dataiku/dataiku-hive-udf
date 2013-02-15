package com.dataiku.hive.udf.arrays;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/** 
 * Joins an array of arrays into a single array containing all elements.
 * No deduplication is performed
 */
public class UDFArrayJoin extends GenericUDF {
    ListObjectInspector arrayInspector;
    ListObjectInspector elementsInspector;

    List<Object> ret = new ArrayList<Object>();

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args.length != 1) return null;
        Object oin = args[0].get();
        if (oin == null) return null;

        int nbArrays = arrayInspector.getListLength(oin);

        ret.clear();
        for (int i = 0; i < nbArrays; i++) {
            Object oarr = arrayInspector.getListElement(oin, i);
            int nbElts = elementsInspector.getListLength(oarr);
            for (int j = 0; j < nbElts; j++) {
                Object oelt = elementsInspector.getListElement(oarr, j);
                ret.add(oelt);
            }
        }
        return ret;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "array_join(" + args[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("array_join expects 1 argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("array_join expects an array as argument, got " + args[0].getTypeName());
        }
        arrayInspector = (ListObjectInspector) args[0];

        ObjectInspector tmpElementsInspector = arrayInspector.getListElementObjectInspector();
        if (tmpElementsInspector.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("array_join expects array<array>, got array<" + tmpElementsInspector.getTypeName() + ">");
        }
        elementsInspector = (ListObjectInspector)tmpElementsInspector;

        ObjectInspector elementElementInspector = elementsInspector.getListElementObjectInspector();

        return ObjectInspectorFactory.getStandardListObjectInspector(elementElementInspector);
    }
}