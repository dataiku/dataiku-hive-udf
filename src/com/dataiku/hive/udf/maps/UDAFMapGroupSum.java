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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.rmi.MarshalledObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Group a set of map and sum identical integer keys
 */
public class UDAFMapGroupSum extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {
        if (tis.length != 1) {
            throw new UDFArgumentTypeException(tis.length - 1, "Exactly one argument is expected.");
        }
        if (tis[0].getTypeName().equals("map<string,int>")) {
            return new MapGroupSumEvaluator();
        } else if (tis[0].getTypeName().equals("map<string,bigint>")) {
            return new MapGroupSumLongEvaluator();
        } else if (tis[0].getTypeName().equals("map<string,double>")) {
            return new MapGroupSumDoubleEvaluator();
        } else if (tis[0].getTypeName().equals("map<string,float>")) {
            return new MapGroupSumFloatEvaluator();
        } else {
            throw new UDFArgumentTypeException(0,
                "Only supports map<string,int>, map<string,bigint>, map<string,float>, and map<string, double>.  Got: '" + tis[0].getTypeName() + "'");
        }
    }

    public static class MapGroupSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private IntObjectInspector valueOI;
        private StringObjectInspector keyOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (StringObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (IntObjectInspector) originalDataOI.getMapValueObjectInspector();
            return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector);



//            /* Setup input OI */
//            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
//                /* Input is original data */
//                originalDataOI = parameters[0];
//            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
//                /* Input is list of original data */
//                listOI = (StandardListObjectInspector) parameters[0];
//                originalDataOI = listOI.getListElementObjectInspector();
//            }
//
//            /* Output OI : always a list of original data */
//            return ObjectInspectorFactory
//                    .getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(originalDataOI));
        }

        static class MapBuffer implements AggregationBuffer {
            Map<String, Integer> map = new HashMap<String, Integer>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<String, Integer> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }
            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                Object okey = entry.getKey();
                Object ovalue = entry.getValue();
                if (okey == null || ovalue == null) continue;
                String key = keyOI.getPrimitiveJavaObject(entry.getKey());
                Integer value = valueOI.get(entry.getValue());
                if (m.containsKey(key)) {
                    m.put(key, m.get(key) + value);
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
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
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
            return Maps.newHashMap(agg.map);
        }
    }
    
    
    public static class MapGroupSumLongEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private LongObjectInspector valueOI;
        private StringObjectInspector keyOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (StringObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (LongObjectInspector) originalDataOI.getMapValueObjectInspector();
            return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaLongObjectInspector);



//            /* Setup input OI */
//            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
//                /* Input is original data */
//                originalDataOI = parameters[0];
//            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
//                /* Input is list of original data */
//                listOI = (StandardListObjectInspector) parameters[0];
//                originalDataOI = listOI.getListElementObjectInspector();
//            }
//
//            /* Output OI : always a list of original data */
//            return ObjectInspectorFactory
//                    .getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(originalDataOI));
        }

        static class MapBuffer implements AggregationBuffer {
            Map<String, Long> map = new HashMap<String, Long>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<String, Long> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }
            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                Object okey = entry.getKey();
                Object ovalue = entry.getValue();
                if (okey == null || ovalue == null) continue;
                String key = keyOI.getPrimitiveJavaObject(entry.getKey());
                Long value = valueOI.get(entry.getValue());
                if (m.containsKey(key)) {
                    m.put(key, m.get(key) + value);
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
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
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
            return Maps.newHashMap(agg.map);
        }
    }


    public static class MapGroupSumDoubleEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private DoubleObjectInspector valueOI;
        private StringObjectInspector keyOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (StringObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (DoubleObjectInspector) originalDataOI.getMapValueObjectInspector();
            return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);



//            /* Setup input OI */
//            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
//                /* Input is original data */
//                originalDataOI = parameters[0];
//            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
//                /* Input is list of original data */
//                listOI = (StandardListObjectInspector) parameters[0];
//                originalDataOI = listOI.getListElementObjectInspector();
//            }
//
//            /* Output OI : always a list of original data */
//            return ObjectInspectorFactory
//                    .getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(originalDataOI));
        }

        static class MapBuffer implements AggregationBuffer {
            Map<String, Double> map = new HashMap<String, Double>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<String, Double> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }
            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                Object okey = entry.getKey();
                Object ovalue = entry.getValue();
                if (okey == null || ovalue == null) continue;
                String key = keyOI.getPrimitiveJavaObject(entry.getKey());
                Double value = valueOI.get(entry.getValue());
                if (m.containsKey(key)) {
                    m.put(key, m.get(key) + value);
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
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
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
            return Maps.newHashMap(agg.map);
        }
    }


    public static class MapGroupSumFloatEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private FloatObjectInspector valueOI;
        private StringObjectInspector keyOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (StringObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (FloatObjectInspector) originalDataOI.getMapValueObjectInspector();
            return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaFloatObjectInspector);



//            /* Setup input OI */
//            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
//                /* Input is original data */
//                originalDataOI = parameters[0];
//            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
//                /* Input is list of original data */
//                listOI = (StandardListObjectInspector) parameters[0];
//                originalDataOI = listOI.getListElementObjectInspector();
//            }
//
//            /* Output OI : always a list of original data */
//            return ObjectInspectorFactory
//                    .getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(originalDataOI));
        }

        static class MapBuffer implements AggregationBuffer {
            Map<String, Float> map = new HashMap<String, Float>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<String, Float> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }
            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                Object okey = entry.getKey();
                Object ovalue = entry.getValue();
                if (okey == null || ovalue == null) continue;
                String key = keyOI.getPrimitiveJavaObject(entry.getKey());
                Float value = valueOI.get(entry.getValue());
                if (m.containsKey(key)) {
                    m.put(key, m.get(key) + value);
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
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
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
            return Maps.newHashMap(agg.map);
        }
    }
}
