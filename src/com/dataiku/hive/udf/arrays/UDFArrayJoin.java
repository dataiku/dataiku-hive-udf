/**
 * Copyright 2013 Dataiku
 * 
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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