package com.dataiku.hive.udf.maths;

/**
 * Author: Matthieu Scordia
 * Date: 04/03/14
 * Time: 15:12
 *
 * This class is call by UDFExponentialSmoothingMovingAverage to do the moving average.
 *
 */


import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrefixSumMovingAverage {
    static class PrefixSumEntry implements Comparable
    {
        int period;
        double value;
        double prefixSum;
        double subsequenceTotal;
        double movingAverage;
        public int compareTo(Object other)
        {
            PrefixSumEntry o = (PrefixSumEntry)other;
            if (period < o.period)
                return -1;
            if (period > o.period)
                return 1;
            return 0;
        }
    }

    //class variables
    private int windowSize;
    private double diviseur;
    private int position;

    private ArrayList<PrefixSumEntry> entries;

    public PrefixSumMovingAverage()
    {
        windowSize = 0;
        diviseur = 0.0;
        position = 0;
    }

    public void reset()
    {
        windowSize = 0;
        diviseur = 0.0;
        position = 0;
        entries = null;
    }

    public boolean isReady()
    {
        return (windowSize > 0);
    }

    /**
     * Sets the window for prefix sum computations
     *
     * @param window_size Size of the window for moving average
     *        d is the divisor of the exponential smoothing.
     */
    public void allocate(int window_size, double d, int p) {

        windowSize = window_size;
        diviseur = d;
        entries = new ArrayList<PrefixSumEntry>();
        position = p;
    }

    public double getDiviseur() {
        return diviseur;
    }

    public double getPosition() {
        return position;
    }


    @SuppressWarnings("unchecked")
    public void merge(List<DoubleWritable> other)
    {

        if (other == null)
            return;

        // if this is an empty buffer, just copy in other
        // but deserialize the list
        if (windowSize == 0)
        {

            windowSize = (int)other.get(0).get();
            diviseur = (double)other.get(1).get();
            position = (int)other.get(2).get();




            entries = new ArrayList<PrefixSumEntry>();
            // we're serialized as period, value, period, value
            for (int i = 3; i < other.size(); i+=2)
            {
                PrefixSumEntry e = new PrefixSumEntry();
                e.period = (int)other.get(i).get();
                e.value = other.get(i+1).get();
                entries.add(e);
            }
        }

        // if we already have a buffer, we need to add these entries
        else
        {
            // we're serialized as period, value, period, value
            for (int i = 3; i < other.size(); i+=2)
            {
                PrefixSumEntry e = new PrefixSumEntry();
                e.period = (int)other.get(i).get();
                e.value = other.get(i+1).get();
                entries.add(e);
            }
        }

        // sort and recompute
        Collections.sort(entries);

        // Compute the list of ponderation coeff for the moving average.


        // Compute the list of ponderation coeff for the moving average.

        double subdenom = 0.0;
        double coeffPond;

        int lastEntry = entries.size()-1;
        double prefixSum = 0;
        int variationPos = 0;

        PrefixSumEntry thisEntry = entries.get(lastEntry-variationPos);

        while (thisEntry.period>(getPosition())){
            variationPos+=1;
            thisEntry = entries.get(lastEntry-variationPos);

        };

        for (int i=0; i<windowSize; i++){
            coeffPond = 1/Math.pow(this.getDiviseur(),i+1);
            prefixSum += thisEntry.value * coeffPond;

            subdenom += coeffPond;
            variationPos+=1;
            if(lastEntry-variationPos>=0){
                thisEntry = entries.get(lastEntry-variationPos);
            }
            else{
                break;
            }
        }

        double movingAverage = prefixSum/subdenom; //Moving average is computed here!

        PrefixSumEntry Entry = entries.get(lastEntry);
        Entry.movingAverage = movingAverage;

    }

    public int tableSize()
    {
        return entries.size();
    }

    public PrefixSumEntry getEntry(int index)
    {
        return entries.get(index);
    }

    private boolean needsSorting(ArrayList<PrefixSumEntry> entries)
    {
        PrefixSumEntry previous = null;
        for (PrefixSumEntry current:entries)
        {
            if (previous != null && current.compareTo(previous) < 0)
                return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public void add(int period, double v)
    {
        //Add a new entry to the list and update table
        PrefixSumEntry e = new PrefixSumEntry();
        e.period = period;
        e.value = v;
        entries.add(e);
        // do we need to ensure this is sorted?
        //if (needsSorting(entries))
        Collections.sort(entries);
        // update the table
        // prefixSums first
    }

    public ArrayList<DoubleWritable> serialize()
    {
        ArrayList<DoubleWritable> result = new ArrayList<DoubleWritable>();

        result.add(new DoubleWritable(windowSize));
        result.add(new DoubleWritable(diviseur));
        result.add(new DoubleWritable(position));

        if (entries != null)
        {
            for (PrefixSumEntry i : entries)
            {
                result.add(new DoubleWritable(i.period));
                result.add(new DoubleWritable(i.value));
            }
        }
        return result;
    }
}
