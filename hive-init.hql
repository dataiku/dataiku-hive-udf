ADD JAR dist/dataiku-hive-udf.jar;

CREATE TEMPORARY FUNCTION count_distinct_map as 'com.dataiku.hive.udf.maps.UDAFCountDistinctToMap';