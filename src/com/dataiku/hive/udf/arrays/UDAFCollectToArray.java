package com.dataiku.hive.udf.arrays;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class UDAFCollectToArray extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {
        if (tis.length != 1) {
            throw new UDFArgumentTypeException(tis.length - 1, "Exactly one argument is expected.");
        }
        return new CollectToArrayEvaluator();
    }

    public static class CollectToArrayEvaluator extends GenericUDAFEvaluator {
        private ObjectInspector originalDataOI;
        private StandardListObjectInspector listOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            /* Setup input OI */
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                /* Input is original data */
                originalDataOI = parameters[0];
            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
                /* Input is list of original data */
                listOI = (StandardListObjectInspector) parameters[0];
                originalDataOI = listOI.getListElementObjectInspector();
            }

            /* Output OI : always a list of original data */
            return ObjectInspectorFactory
                    .getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(originalDataOI));
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {
            ArrayList<Object> container = new ArrayList<Object>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((ArrayAggregationBuffer) ab).container.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new ArrayAggregationBuffer();
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
                agg.container.add(ObjectInspectorUtils.copyToStandardObject(p, this.originalDataOI));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
            ArrayList<Object> ret = new ArrayList<Object>();
            ret.addAll(agg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
            @SuppressWarnings("unchecked")
            ArrayList<Object> partialResult = (ArrayList<Object>)listOI.getList(p);
            for(Object o : partialResult) {
                agg.container.add(ObjectInspectorUtils.copyToStandardObject(o, originalDataOI));
            }
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
            ArrayList<Object> ret = new ArrayList<Object>();
            ret.addAll(agg.container);
            return ret;
        }
    }
}
