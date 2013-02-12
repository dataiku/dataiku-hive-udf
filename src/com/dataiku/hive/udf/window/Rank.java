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