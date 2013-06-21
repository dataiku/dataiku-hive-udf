package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class UDFMapValueFilterLowerThan extends UDF {

    Set<String> toRemove = new HashSet<String>();

    public Map<String, Integer> evaluate(Map<String, Integer> map, Integer minValue) {

        toRemove.clear();
        for(String s : map.keySet()) {
            if (map.get(s) < minValue) {

                toRemove.add(s);
            }
        }

        for(String s : toRemove) {
            map.remove(s);
        }
        return map;
    }
}
