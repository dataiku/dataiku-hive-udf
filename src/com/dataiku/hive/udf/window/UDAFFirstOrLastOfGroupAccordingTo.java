package com.dataiku.hive.udf.window;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public abstract class UDAFFirstOrLastOfGroupAccordingTo extends AbstractGenericUDAFResolver {
    protected void checkParameters(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.getParameterObjectInspectors().length != 2) {
            throw new UDFArgumentException("Two arguments are required");
        }

        if (info.getParameterObjectInspectors()[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, 
                    "Only primitive type arguments are accepted but "
                            + info.getParameterObjectInspectors()[0].getTypeName() + " was passed in");
        }
        if (info.getParameterObjectInspectors()[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, 
                    "Only primitive type arguments are accepted but "
                            + info.getParameterObjectInspectors()[1].getTypeName() + " was passed in");
        }
    }

    public static abstract class BaseEvaluator extends GenericUDAFEvaluator {
        static class UDAFFOGATBuffer implements AggregationBuffer {
            Object outColKeptValue;
            Object sortColKeptValue;
        }
        
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private ObjectInspector outColOI; // Also used as output inspector for FINAL
        private ObjectInspector sortColOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (structs of objects)
        private StructObjectInspector soi;
        StructField outField;
        StructField sortField;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)  throws HiveException {
            super.init(m, parameters);
           
            /* Init input inspectors. */
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                /* For partial 1 and complete : original data */
                if (parameters.length != 2) throw new UDFArgumentException("It sucks " + parameters.length);
                outColOI = (PrimitiveObjectInspector) (parameters[0]);
                sortColOI = (PrimitiveObjectInspector) (parameters[1]);
            } else  {
                /* For partial2 and final : struct containing partial results */
                if (parameters.length != 1) throw new UDFArgumentException("It sucks " + parameters.length);
                soi = (StructObjectInspector) parameters[0];
                outField = soi.getStructFieldRef("out");
                outColOI = outField.getFieldObjectInspector();
                sortField = soi.getStructFieldRef("sort");
                sortColOI = sortField.getFieldObjectInspector();
            }                

            /* Init output inspectors */
            if (m == Mode.FINAL || m == Mode.COMPLETE) {
                return outColOI;
            } else {
                /* The output of a partial aggregation is a struct containing the best sort value and the best out value */
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(ObjectInspectorUtils.getStandardObjectInspector(outColOI, ObjectInspectorCopyOption.WRITABLE));
                foi.add(ObjectInspectorUtils.getStandardObjectInspector(sortColOI, ObjectInspectorCopyOption.WRITABLE));

                ArrayList<String> fname = new ArrayList<String>();
                fname.add("out");
                fname.add("sort");

                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer o= new UDAFFOGATBuffer();
            //System.out.println(Thread.currentThread().getName() + ": NEW BUFFER " +o);
            return o;
        }

        @Override
        public void reset(AggregationBuffer buf) throws HiveException {
            UDAFFOGATBuffer bbuf = (UDAFFOGATBuffer)buf;
            //System.out.println(Thread.currentThread().getName() + ": RESET");
            bbuf.outColKeptValue = null;
            bbuf.sortColKeptValue = null;
        }

        @Override
        public void iterate(AggregationBuffer buf, Object[] args) throws HiveException {
            UDAFFOGATBuffer bbuf = (UDAFFOGATBuffer)buf;
            Object outColVal = args[0];
            Object sortColVal = args[1];
            //System.out.println(Thread.currentThread().getName() + ": ITERATE ON " + outColVal + " (" + sortColVal + ") to " +bbuf);
            //System.out.println("    BBUF HAS " + bbuf.outColKeptValue+ " " +bbuf.sortColKeptValue);
            updateBuf(bbuf, outColVal, sortColVal);
        }

        @Override
        public void merge(AggregationBuffer buf, Object toMerge) throws HiveException {
            // System.out.println(Thread.currentThread().getName() + ": MERGE " + buf + " --" + toMerge + "--  soi is " +soi);
            UDAFFOGATBuffer bbuf = (UDAFFOGATBuffer)buf;
            Object out = soi.getStructFieldData(toMerge, outField);
            Object sort = soi.getStructFieldData(toMerge, sortField);
            updateBuf(bbuf, out, sort);
        }

        protected abstract boolean needUpdate(int cmp);

        private void updateBuf(UDAFFOGATBuffer bbuf, Object outColVal, Object sortColVal) {
            if (bbuf.sortColKeptValue == null) {
                bbuf.sortColKeptValue = ObjectInspectorUtils.copyToStandardObject(sortColVal, sortColOI, ObjectInspectorCopyOption.WRITABLE);
                bbuf.outColKeptValue = ObjectInspectorUtils.copyToStandardObject(outColVal, outColOI, ObjectInspectorCopyOption.WRITABLE);
            } else {
                int cmp = ObjectInspectorUtils.compare(sortColVal, sortColOI, bbuf.sortColKeptValue,
                        ObjectInspectorUtils.getStandardObjectInspector(sortColOI, ObjectInspectorCopyOption.WRITABLE)
                        );
                if (needUpdate(cmp)) {
                    bbuf.sortColKeptValue = ObjectInspectorUtils.copyToStandardObject(sortColVal, sortColOI, ObjectInspectorCopyOption.WRITABLE);
                    bbuf.outColKeptValue = ObjectInspectorUtils.copyToStandardObject(outColVal, outColOI, ObjectInspectorCopyOption.WRITABLE);
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer buf) throws HiveException {
            UDAFFOGATBuffer bbuf = (UDAFFOGATBuffer)buf;
            return bbuf.outColKeptValue;
        }

        @Override
        public Object terminatePartial(AggregationBuffer buf) throws HiveException {
            UDAFFOGATBuffer bbuf = (UDAFFOGATBuffer)buf;
            Object[] out = new  Object[2];
            out[0] = (bbuf.outColKeptValue);
            out[1] = (bbuf.sortColKeptValue);
            // System.out.println("TERMINATE PARTIAL RETURN " + out[0] +  " -- " + out[1] + " soi is " + soi);
            return out;
        }
    }

}
