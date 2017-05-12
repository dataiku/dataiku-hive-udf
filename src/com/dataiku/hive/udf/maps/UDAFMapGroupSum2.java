package com.dataiku.hive.udf.maps;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;

import java.rmi.MarshalledObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Group a set of map and sum identical double keys
 */
public class UDAFMapGroupSum2 extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {
        if (tis.length != 1) {
            throw new UDFArgumentTypeException(tis.length - 1, "Exactly one argument is expected.");
        }
        return new MapGroupSumEvaluator();
    }

    public static class MapGroupSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private HiveDecimalObjectInspector valueOI;
        private HiveCharObjectInspector keyOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (HiveCharObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (HiveDecimalObjectInspector) originalDataOI.getMapValueObjectInspector();

            int length = ((CharTypeInfo)keyOI.getTypeInfo()).getLength();
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    new JavaHiveCharObjectInspector((CharTypeInfo) new CharTypeInfo(length)),
                    PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
        }

        static class MapBuffer implements AggregationBuffer {
            Map<HiveChar, HiveDecimal> map = new HashMap<HiveChar, HiveDecimal>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<HiveChar, HiveDecimal> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }
            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                Object okey = entry.getKey();
                Object ovalue = entry.getValue();
                if (okey == null || ovalue == null) continue;
                HiveChar key = keyOI.getPrimitiveJavaObject(entry.getKey());
                HiveDecimal value = valueOI.getPrimitiveJavaObject(entry.getValue());
                if (m.containsKey(key)) {
                    m.put(key, value.add(m.get(key)));
                } else {
                    m.put(key, value);
                }
            }
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MapBuffer agg = (MapBuffer) ab;
                Map<Object, Object> o = (Map<Object, Object>) this.originalDataOI.getMap(p);
                mapAppend(agg.map, o);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            return terminate(ab);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) this.originalDataOI.getMap(p);
            mapAppend(agg.map, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            Map<HiveChar, HiveDecimal> result = new HashMap<HiveChar, HiveDecimal>();
            
            for(Map.Entry<HiveChar, HiveDecimal> entry : agg.map.entrySet()) {
                HiveChar oKey = entry.getKey();
                HiveDecimal oValue = entry.getValue();

                oKey = new HiveChar(oKey.toString().trim(), 3);

                result.put(oKey, oValue);
            }

            return result;
        }
    }
}
