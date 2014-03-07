# Dataiku Hive UDFs

This is a collection of UDF and Storage Handlers for [Apache Hive](http://apache.hive.org).

## Available UDFs

### Array operations

#### array_count_distinct

    int array_count_distinct(array<string>)

Counts the number of distinct values in the array

#### array_count_equals

    int array_count_equals(array<int> haystack, int needle)
    int array_count_equals(array<double> haystack, double needle)
    int array_count_equals(array<string> haystack, string needle)

Returns the number of times the needle is present in the haystack

#### collect_to_array

This is an aggregation function that gathers all input values and outputs them as an array.

For example

    table page_views {
        int visitor_id;
        string page;
    }

The query:

    select collect_to_array(page) from page_views group by visitor_id;

produces: `array<string>`, the list of pages viewed for each visitor_id

#### array_join

    array<TYPE> array_join(array<array<TYPE> >)

Joins an array of arrays into a single array containing all elements.
This is often used in combination with collect_to_array

For example, if you have:

    table A {
        int product_id;
        int day;
        array<string> buying_customers;
    }

collect_to_array(buying_customers) will therefore produce array<array<string>>

To get the full list of customers for one product, you can use:

    SELECT array_join(collect_to_array(buying_customers)) FROM A GROUP BY product_id;

### Map operations

#### count_to_map

Convert an array<string> to a map<string,int>, with a count of number of elements


    count_to_map<["yes", "no", "yes"]>  => {"yes":2,"no":1}

### count_distinct_map

For a group, generate a map with key from a secondary column counting the distinct values from keys from a third one. 


    select query, count_distinct_map(country, userid) as nusers_per_country FROM queries GROUP BY query; 

    query     country    userid
    FOO    FR    X
    FOO    FR    X
    FOO    FR    Y
    FOO    EN    Z 

    =>  FOO,  {"FR":2, EN:1}


### map_filter_lower_than

Filter a map<string,int>, keep only map entries where value is greater of equals to the provided argument

    map_filter_lower_than({"yes":2, "no":1}, 2) => {"yes":2}


### map_filter_top_n

Filter a map<string, int>, keep only the top N map entries according to the value. In case of equality, a random
selection of the elements is performed

    map_filter_top_n({"yes":2, "no":1, "maybe":2, "surely":5}, 3) => {"surely":5, "maybe":2, "yes":2}

## map_group_sum

Aggregating operation on map<string,int> than performs the unions of keys of the map, and sum the value when a key
exists in multiples maps


    CREATE TABLE docs {
        docid int;
        word_count map<string, int>
    }

    SELECT map_group_sum(word_count) FROM docs; ## Get the global word frequency

### Maths

### UDFExponentialSmoothingMovingAverage.

    moving_avg(period, value, window, divisor)
Compute the moving average on a column.

Example:

    p   v
    4	40
    5	60
    6	0
    7	10
    8	20
    9	50
    10	100
    11	10

moving_avg(p, v, 4, 2) return:
    mean(10 * 1/(2^1) + 100 * 1/(2^2) +  50 * 1/(2^3) +  20 * 1/(2^4))

moving_avg(p, v, 2, 3) return:
    mean(10 * 1/(3^1) + 100 * 1/(3^2))


### Windowing functions

#### Rank

    int rank(string in)
    
While processing a stream of rows, rank will return the number of times it has previously seen the same value of `in`.

For example, while processing a table:
   
    table a {
        string data;
    }
    
with values:

    p1
    p1
    p2
    p2
    p2
    p3
    p4

The query:

    select data, rank(data) from a;

would return:

    p1   0
    p1   1
    p2   0
    p2   1
    p2   2
    p3   0
    p4   0
    
Therefore, rank only makes sense on a sorted table.

rank is very useful for sequence analysis

##### first_of_group, last_of_group

This is an aggregation function.

    TYPE1 first_of_group(TYPE1 outColumn, TYPE2 sortColumn)
    TYPE1 last_of_group(TYPE1 outColumn, TYPE2 sortColumn)
    
For each group, these functions will sort the rows of the group by `sortColumn`, and then 
output the value of `outColumn` for the first (resp. last) row, once sorted.

These functions are very useful for processing tables with "updates".

For example:

    table user {
        int id;
        int version;
        string email;
        string location;
    }

To get the last recorded location for a given user, you can use:

    select last_of_group(location, version) FROM user GROUP BY id;

You can use several first_of_group/last_of_group in the same query:

    select last_of_group(location, version), last_of_group(email, version) FROM user GROUP BY id;



## Storage Handlers

### XMLHiveStorageHandler


XMLHiveStorageHandler creates a table backed by one or multiple XML Files.

In the example below my_dir should contain XML Files contains a <MyTag> tag.
A Table will be created with one line per tag, with the raw XML content of each tag inside.

    CREATE TABLE my_table (text string)
    STORED BY 'com.dataiku.hive.storage.XMLHiveStorageHandler'
    LOCATION '/my_dir'
    TBLPROPERTIES (
        "xml.tag"="MyTag"
    )

Note that the storage handler does not perform any XML entity substitution (such as &gt; or unicode entities)

qaa
## Copyright and license

Copyright 2013 Dataiku SAS.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. 
