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

@Description(name="array_count_equals", value="_FUNC_(array<type>, type needle) - Counts the number of times the needle appears in the array")
public class UDFArrayCountEquals extends UDF {
    public int evaluate(List<String> a, String needle) {
        if (a == null) return 0;
        if (needle == null) return a.size();

        int ret = 0;
        for (int i = 0; i < a.size(); i++) {
            if (needle.equals(a.get(i))) ret++;
        }
        return ret;
    }
    
    public int evaluate(List<Integer> a, int needle) {
        if (a == null) return 0;

        int ret = 0;
        for (int i = 0; i < a.size(); i++) {
            if (needle == a.get(i)) ret++;
        }
        return ret;
    }
    
    public double evaluate(List<Double> a, double needle) {
        if (a == null) return 0;

        int ret = 0;
        for (int i = 0; i < a.size(); i++) {
            if (needle == a.get(i)) ret++;
        }
        return ret;
    }
}
