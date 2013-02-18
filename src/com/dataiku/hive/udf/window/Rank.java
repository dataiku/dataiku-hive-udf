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
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="rank", value="_FUNC_(string) - Returns the number of times the column had the same value in the previous records")
public final class Rank extends UDF {
    private int counter;
    private String currentKey;
    
    public int evaluate(final String key) {
      if (!key.equalsIgnoreCase(currentKey)) {
         counter = 0;
         currentKey = key;
      }
      return counter++;
    }
}