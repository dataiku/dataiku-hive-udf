package com.dataiku.hive.udf.window;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;

public final class UDAFFirstOfGroupAccordingTo extends UDAFFirstOrLastOfGroupAccordingTo {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        checkParameters(info);
        return new FirstEvaluator();
    }

    public static class FirstEvaluator extends BaseEvaluator {
        @Override
        protected boolean needUpdate(int cmp) {
            return cmp < 0;
        }
    }

}
