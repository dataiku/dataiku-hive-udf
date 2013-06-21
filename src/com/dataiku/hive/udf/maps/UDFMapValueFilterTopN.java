package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * Filter topN Elements from a map
 */
public class UDFMapValueFilterTopN extends UDF {
    public Map<String, Integer> evaluate(Map<String, Integer> map, Integer n) {

        if (map.size() < n) {
            return map;
        }
        List<Integer> list = new ArrayList(map.values());
        Collections.sort(list);
        int limit = list.get(list.size() - n);
        int count = 0;
        HashMap<String, Integer> nm = new HashMap<String, Integer>();

        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > limit) {
                nm.put(entry.getKey(), entry.getValue());
            }
        }
        for(Map.Entry<String, Integer> entry : map.entrySet()) {
             if (nm.size() == n) {
                break;
             }
            if (entry.getValue() == limit) {
                nm.put(entry.getKey(), entry.getValue());
            }
        }
        return nm;
    }
}

