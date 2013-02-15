package com.dataiku.hive.udf.window;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;

@Description(name="last_of_group", value="_FUNC_(outputColumn, sortColumn)")
public final class UDAFLastOfGroupAccordingTo extends UDAFFirstOrLastOfGroupAccordingTo {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        checkParameters(info);
        return new FirstEvaluator();
    }

    public static class FirstEvaluator extends BaseEvaluator {
        @Override
        protected boolean needUpdate(int cmp) {
            return cmp > 0;
        }
    }

}
