package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * Retrieve the key with the maximal value for a map
 */
public class UDFMapMaxKey extends UDF {
    public String evaluate(Map<String, Integer> map) {
        String maxKey = null;
        Integer maxValue = null;
        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            if (maxValue == null || entry.getValue() > maxValue) {
                maxKey = entry.getKey();
                maxValue = entry.getValue();
            }
        }
        return maxKey;
    }
}

