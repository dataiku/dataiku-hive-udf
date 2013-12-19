ADD JAR dist/dataiku-hive-udf.jar;

CREATE TEMPORARY FUNCTION count_distinct_map as 'com.dataiku.hive.udf.maps.UDAFCountDistinctToMap';
CREATE TEMPORARY FUNCTION array_count_to_map as 'com.dataiku.hive.udf.maps.UDFCountToMap';
CREATE TEMPORARY FUNCTION map_filter_top as 'com.dataiku.hive.udf.maps.UDFMapValueFilterTopN';
CREATE TEMPORARY FUNCTION collect_all as 'com.dataiku.hive.udf.arrays.UDAFCollectToArray';