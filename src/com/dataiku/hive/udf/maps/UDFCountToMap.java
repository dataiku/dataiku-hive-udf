package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

public class UDFCountToMap extends UDF {


    public Map<String, Integer> evaluate(List<String> a) {
        HashMap<String, Integer> map= new HashMap<String, Integer>();
        if (a == null) {
            return null;
        }
        for(String s : a) {
            if (s == null) {
                continue;
            }
            if (map.containsKey(s)) {
                map.put(s, map.get(s) + 1);
            } else {
                map.put(s, 1);
            }
        }
        return map;
    }

}
