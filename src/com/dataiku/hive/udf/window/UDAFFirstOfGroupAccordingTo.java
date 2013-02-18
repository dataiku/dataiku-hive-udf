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
package com.dataiku.hive.udf.window;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;

@Description(name="first_of_group", value="_FUNC_(outputColumn, sortColumn)")
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
