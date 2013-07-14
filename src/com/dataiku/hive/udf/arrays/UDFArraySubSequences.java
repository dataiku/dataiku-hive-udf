/**
 * Copyright 2013 Dataiku
 * 
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataiku.hive.udf.arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

@Description(name="array_get", value="_FUNC_(array<text>, N, MAX) - returns an array<array<text>> with all the subsequences of length <= N from the array")
public class UDFArraySubSequences extends UDF {
    private Text out = new Text();



    public static <T> List<List<T>> mysubn(int n, int max, List<T> li) {
        List<List<T>> ret = new ArrayList<List<T>>();
        int size = li.size();
        if (n == 0) {
            ret.add(new ArrayList<T>());
            return ret;
        }
        if (li.isEmpty()) {
            return ret;
        }
        if (size < n) {
            return ret;
        }
        if (size == n) {
            ret.add(li);
            return ret;
        }

        /* I use counters to actually keep track of where I am in the list,
           * but iterators to actually get the elements.  This is because we can't
           * assume what type of list we are accessing.  list.get(n) is O(1) for
           * ArrayList, but O(n) for LinkedList, so we'd like to minimize these
           * as much as possible.
           * The reason we need to keep the counters and ending array,
           * instead of going until the iterators run out of elements,
           * is we only want them to the point where they could still make a
           * subsequence.
           */
        ArrayList<ListIterator<T>> iters = new ArrayList<ListIterator<T>>(n);
        ArrayList<T> currElems = new ArrayList<T>(n);
        int[] counters = new int[n];
        int[] endings = new int[n];
        // Set up our initial values
        for(int i = 0; i < n; i++) {
            iters.add(li.listIterator(i));
            currElems.add(iters.get(i).next());
            counters[i] = i;
            endings[i] = size - n + i;
        }
        // Go until the we don't have enough elements left to make a subsequence
        while(counters[0] <= endings[0] && ret.size() < max) {
            List<T> sub = new ArrayList<T>();
            for(int i = 0; i < n; i++) {
                sub.add(currElems.get(i));
            }
            ret.add(sub);
            int c = n - 1;
            // Here we figure out how many of the counters (indexes) need updating.
            while(c > 0) {
                if(counters[c] < endings[c])
                    break;
                else
                    c--;
            }
            // Update the left-most counter (index)
            counters[c]++;
            if(iters.get(c).hasNext())
                currElems.set(c, iters.get(c).next());
            c++;
            // Starting from the next left-most counter (if there is one),
            // set counter to be 1 more than previous counter, and reset
            // the iterator to start there as well.
            while(c < n) {
                // Reset the counter, and reset the iterator
                // to the new starting position.
                counters[c] = counters[c-1] + 1;
                iters.set(c, li.listIterator(counters[c]));
                // We make sure the iterator has another element.
                // The only time this will return false is when we are completely done
                // and the outer while is over.
                if(iters.get(c).hasNext())
                    currElems.set(c, iters.get(c).next());
                c++;
            }
        }
        return ret;
    }


    public List<List<String>> evaluate(List<String> a, int n, int max) {
        return mysubn(n, max, a);
    }
}
