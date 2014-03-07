package com.dataiku.hive.udf.maths;

/**
 * Author: Matthieu Scordia
 * Date: 04/03/14
 * Time: 15:12
 *
 * This class is call by UDFExponentialSmoothingMovingAverage to do the moving average.
 *
 */


import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

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

    private ArrayList<PrefixSumEntry> entries;

    public PrefixSumMovingAverage()
    {
        windowSize = 0;
        diviseur = 0;
    }

    public void reset()
    {
        windowSize = 0;
        diviseur = 0.0;
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
    public void allocate(int window_size, double d) {

        windowSize = window_size;
        diviseur = d;
        entries = new ArrayList<PrefixSumEntry>();
    }

    public double getDiviseur() {
        return diviseur;
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
            diviseur = (int)other.get(1).get();




            entries = new ArrayList<PrefixSumEntry>();
            // we're serialized as period, value, period, value
            for (int i = 2; i < other.size(); i+=2)
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
            for (int i = 2; i < other.size(); i+=2)
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

        double[] listCoeff = new double[windowSize];
        double subdenom = 0.0;
        double coeffPond = 0.0;

        for (int i=1; i<=windowSize; i++){
            coeffPond = 1/Math.pow(this.getDiviseur(),i);
            listCoeff[i-1]=coeffPond;
            subdenom += coeffPond;
        }

        // now do the subsequence totals and moving averages

        for(int i = entries.size()-1; i < entries.size(); i++)
        {

            double prefixSum = 0;

            for(int j = 0; j < windowSize; j++)
            {
                if (i-j>=0){
                    PrefixSumEntry thisEntry = entries.get(i-j);
                    prefixSum += thisEntry.value * listCoeff[j];

                }
                else{
                    PrefixSumEntry thisEntry = entries.get(i);
                    prefixSum += thisEntry.value * listCoeff[j];
                }

            }

            double movingAverage;
            PrefixSumEntry thisEntry = entries.get(i);
            movingAverage = prefixSum/subdenom; //Moving average is computed here!
            thisEntry.movingAverage = movingAverage;

        }

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

        // Compute the list of ponderation coeff for the moving average.

        double[] listCoeff = new double[windowSize];
        double subdenom = 0.0;
        double coeffPond = 0.0;

        for (int i=1; i<=windowSize; i++){
            coeffPond = 1/Math.pow(this.getDiviseur(),i);
            listCoeff[i-1]=coeffPond;
            subdenom += coeffPond;
        }

        // now do the subsequence totals and moving averages

        for(int i = entries.size()-1; i < entries.size(); i++)
        {

            double prefixSum = 0;

            for(int j = 0; j < windowSize; j++)
            {
                if (i-j>=0){
                    PrefixSumEntry thisEntry = entries.get(i-j);
                    prefixSum += thisEntry.value * listCoeff[j];

                }
                else{
                    PrefixSumEntry thisEntry = entries.get(i);
                    prefixSum += thisEntry.value * listCoeff[j];
                }

            }

            double movingAverage;
            PrefixSumEntry thisEntry = entries.get(i);
            movingAverage = prefixSum/subdenom; //Moving average is computed here!
            thisEntry.movingAverage = movingAverage;

        }
    }

    public ArrayList<DoubleWritable> serialize()
    {
        ArrayList<DoubleWritable> result = new ArrayList<DoubleWritable>();

        result.add(new DoubleWritable(windowSize));
        result.add(new DoubleWritable(diviseur));

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