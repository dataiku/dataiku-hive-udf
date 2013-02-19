# Dataiku Hive UDFs

This is a collection of UDF for [Apache Hive](http://apache.hive.org).

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

## Copyright and license

Copyright 2013 Dataiku SAS.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. 
