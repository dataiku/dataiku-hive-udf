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
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="array_int_sum", value="_FUNC_(array<int>) - returns the sum of elements in the array")
public class UDFArrayIntSum extends UDF {
    public int evaluate(List<Integer> a) {
        if (a == null) return 0;
        int sum = 0;
        for (int i  = 0; i < a.size(); i++) {
            Integer elt = a.get(i);
            if (elt != null) sum += elt;
        }
        return sum;
    }
}
