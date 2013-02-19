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


## Copyright and license

Copyright 2013 Dataiku SAS.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. 
