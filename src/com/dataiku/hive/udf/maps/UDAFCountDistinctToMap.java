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
package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDAFCountDistinctToMap extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {
        if (tis.length != 2) {
            throw new UDFArgumentTypeException(tis.length - 1, "Exactly two arguments is expected.");
        }
        return new CountDistinctToMap();
    }

    /**
     *  count_distinct_to_map(K key, V value)
     * (key, value) - PARTIAL1 -->  Map<key, Value[]>
     * Map<key, Value[]> - PARTIAL2 --> Map<key, Value[]>
     * Map<key, Value[]> - FINAL --> Map<key, Int>
     * (key, value) - COMPLETE --> Map<key, Int>
     */
    public static class CountDistinctToMap extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector keyTypeOI;
        private PrimitiveObjectInspector valueTypeOI;
        private AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;
        private AbstractPrimitiveWritableObjectInspector valueOutputTypeOI;
        private ListObjectInspector valueListInputTypeOI;
        private StandardListObjectInspector valueListOutputTypeOI;
        private MapObjectInspector intermediateMapInputTypeOI;
        private StandardMapObjectInspector intermediateMapOutputTypeOI;
        private StandardMapObjectInspector finalMapTypeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
         /* Setup input OI */
            if (m == Mode.PARTIAL1) {
                keyTypeOI= (PrimitiveObjectInspector) parameters[0];
                valueTypeOI  = (PrimitiveObjectInspector) parameters[1];
                keyOutputTypeOI =  (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyTypeOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
                valueOutputTypeOI=  (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(valueTypeOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);

                valueListOutputTypeOI = ObjectInspectorFactory.getStandardListObjectInspector(valueOutputTypeOI);
                intermediateMapOutputTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI, valueListOutputTypeOI);
                return intermediateMapOutputTypeOI;
            } else if (m == Mode.COMPLETE) {
                keyTypeOI= (PrimitiveObjectInspector) parameters[0];
                valueTypeOI  = (PrimitiveObjectInspector) parameters[1];
                keyOutputTypeOI =  (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyTypeOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
                valueOutputTypeOI=  (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(valueTypeOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);

                finalMapTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI, PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                /* Input is original data */
                return finalMapTypeOI;
            } else if (m == Mode.PARTIAL2) {
                intermediateMapInputTypeOI = (MapObjectInspector) parameters[0];
                keyOutputTypeOI= (AbstractPrimitiveWritableObjectInspector) intermediateMapInputTypeOI.getMapKeyObjectInspector();
                valueListInputTypeOI= (ListObjectInspector) intermediateMapInputTypeOI.getMapValueObjectInspector();
                valueOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) valueListInputTypeOI.getListElementObjectInspector();
                valueListOutputTypeOI = ObjectInspectorFactory.getStandardListObjectInspector(valueOutputTypeOI);
                intermediateMapOutputTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI, valueListOutputTypeOI);
                return intermediateMapOutputTypeOI;
            } else if (m == Mode.FINAL){
                intermediateMapInputTypeOI = (MapObjectInspector) parameters[0];
                keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) intermediateMapInputTypeOI.getMapKeyObjectInspector();
                valueListInputTypeOI= (ListObjectInspector) intermediateMapInputTypeOI.getMapValueObjectInspector();
                valueOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) valueListInputTypeOI.getListElementObjectInspector();
                valueListOutputTypeOI = ObjectInspectorFactory.getStandardListObjectInspector(valueOutputTypeOI);
                intermediateMapOutputTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI, valueListOutputTypeOI);
                finalMapTypeOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                return finalMapTypeOI;
            } else {
                throw new IllegalArgumentException("Invalid mode");
            }
        }

        class MapAgg implements AggregationBuffer {
            Map<Object, ArrayList<Object>> content;

            protected MapAgg() {
                content = new HashMap<Object, ArrayList<Object>>();
            }

            protected void reset() {
                content.clear();
            }

            protected void addEntry(Object k, Object v) {
                if (v == null) {
                    return;
                }

                ArrayList<Object> vv = content.get(k);
                if (vv == null) {
                    vv = new ArrayList<Object>();
                    content.put(k, vv);

                }
                if (!vv.contains(v)) {
                    vv.add(v);
                }

            }

            protected void iterate(Object[] parameters) {
                Object key = parameters[0];
                Object value = parameters[1];
                addEntry(keyOutputTypeOI.copyObject(keyTypeOI.getPrimitiveWritableObject(key)),
                        valueOutputTypeOI.copyObject(valueTypeOI.getPrimitiveWritableObject(value)));
            }

            protected Object terminatePartial() {
                return content;
            }

            protected void merge(Object o) {
                Map<?, ?> map = intermediateMapInputTypeOI.getMap(o);
                for(Map.Entry<?,?> entry: map.entrySet()) {
                    Object k = entry.getKey();
                    Object v = entry.getValue();
                    for(Object oo : valueListInputTypeOI.getList(v)) {
                        addEntry(keyOutputTypeOI.copyObject(k), valueOutputTypeOI.copyObject(oo));
                    }
                }
            }

            protected Object terminate() {
                Map<Object,ArrayList<Object>> map = content;
                Map<Object, Integer> mapFinal = new HashMap<Object, Integer>();

                for(Map.Entry<Object, ArrayList<Object>> entry : content.entrySet()) {
                    mapFinal.put(entry.getKey(), Integer.valueOf(entry.getValue().size()));
                }
                return mapFinal;
            }

        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapAgg) ab).reset();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapAgg();
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 2);
            ((MapAgg) ab).iterate(parameters);
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            return ((MapAgg) ab).terminatePartial();
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            ((MapAgg) ab).merge(p);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            return ((MapAgg) ab).terminate();
        }
    }
}
