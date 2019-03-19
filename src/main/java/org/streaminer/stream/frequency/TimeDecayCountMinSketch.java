package org.streaminer.stream.frequency;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import com.google.common.util.concurrent.AtomicDoubleArray;
import org.streaminer.stream.frequency.decay.DecayFormula;
import org.streaminer.util.hash.HashUtils;

/**
 * Implementaion of a time decaying Count-Min Sketch with values updated on-demand
 * instead of fixed time intervals. The Count-Min Sketch implementation is from
 * the CountMinSketchAlt class, originally from the stream-lib. The Decay functions
 * were obtained from a <a href="https://github.com/michal-harish/streaming-sketches">DecayHashMap implementation</a>.
 */
public class TimeDecayCountMinSketch implements ITimeDecayFrequency<Object> {
    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private int depth;
    private int width;
    private AtomicDoubleArray[] table;
    private long[] hashA;
    private AtomicLongArray timers;
    private AtomicLong size;
    private double eps;
    private double confidence;
    private DecayFormula formula;

    private TimeDecayCountMinSketch() {
    }

    public TimeDecayCountMinSketch(int depth, int width, int seed, DecayFormula formula) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.formula = formula;
        this.size = new AtomicLong();
        initTablesWith(depth, width, seed);
    }

    public TimeDecayCountMinSketch(double epsOfTotalCount, double confidence, int seed, DecayFormula formula) {
        // 2/w = eps ; w = 2/eps
        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        this.formula = formula;
        this.size = new AtomicLong();
        initTablesWith(depth, width, seed);
    }

    private TimeDecayCountMinSketch(int depth, int width, int size, long[] hashA, AtomicDoubleArray[] table) {
        this.depth = depth;
        this.width = width;
        this.eps   = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashA = hashA;
        this.table = table;
        this.size  = new AtomicLong(size);
    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new AtomicDoubleArray[depth];
        for (int i = 0; i < table.length; i++)
        {
            table[i] = new AtomicDoubleArray(width);
        }

        this.hashA = new long[depth];
        this.timers = new AtomicLongArray(width);

        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    private int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    public void add(Object item, long qtd, long timestamp) {
        if (qtd < 0) {
            throw new IllegalArgumentException("Negative increments not implemented");
        }

        if (item instanceof Integer) {
            addLong(((Integer)item).longValue(), qtd, timestamp);
        } else if (item instanceof Long) {
            addLong((Long)item, qtd, timestamp);
        } else if (item instanceof String) {
            addString((String)item, qtd, timestamp);
        }
    }

    public void addString(String item, long qtd, long timestamp) {
        int[] buckets = HashUtils.getHashBuckets((String)item, depth, width);

        for (int i = 0; i < depth; ++i) {
            int h = buckets[i];

            addHashForPosition(qtd, h, i, timestamp);
        }

        size.addAndGet(qtd);
    }

    private void addLong(long item, long qtd, long timestamp) {
        for (int i = 0; i < depth; ++i) {
            int h = hash((Long)item, i);

            addHashForPosition(qtd, h, i, timestamp);
        }
        size.addAndGet(qtd);
    }

    private void addHashForPosition(long qtd, int h, int i, long timestamp)
    {
        while (true)
        {
            long oldTimestamp = timers.get(h);

            AtomicDoubleArray subArray = table[i];
            if (oldTimestamp <= timestamp)
            {
                boolean timestampSet = timers.compareAndSet(h, oldTimestamp, timestamp);
                if (!timestampSet)
                {
                    // Try again
                    continue;
                }

                // We won this timestamp update. Now we get to reduce the old value
                double oldCount = subArray.get(h);
                double delta = projectValue(timestamp, oldTimestamp, oldCount) + qtd - oldCount;
                subArray.addAndGet(h, delta);
            }
            else
            {
                subArray.addAndGet(h, projectValue(oldTimestamp, timestamp, qtd));
            }
            break;
        }
    }

    public double estimateCount(Object item, long timestamp) {
        if (item instanceof Integer) {
            return estimateCountLong(((Integer)item).longValue(), timestamp);
        } else if (item instanceof Long) {
            return estimateCountLong((Long)item, timestamp);
        } else if (item instanceof String) {
            return estimateCountString((String) item, timestamp);
        }

        return 0d;
    }

    public double estimateCountString(String item, long timestamp) {
        double res = Double.MAX_VALUE;
        int[] buckets = HashUtils.getHashBuckets((String)item, depth, width);
        for (int i = 0; i < depth; ++i) {
            double value = projectValue(timestamp, timers.get(buckets[i]), table[i].get(buckets[i]));
            res = Math.min(res, value);
        }
        return res;
    }

    private double estimateCountLong(long item, long timestamp) {
        double res = Double.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            int h = hash((Long)item, i);
            double value = projectValue(timestamp, timers.get(h), table[i].get(h));
            res = Math.min(res, value);
        }
        return res;
    }

    private double projectValue(long futureTimestamp, long timestamp, double quantity) {
        if (futureTimestamp < timestamp) {
            throw new IllegalArgumentException("Cannot project decaying quantity into the past.");
        }
        double t = Double.valueOf(futureTimestamp -  timestamp);
        return formula.evaluate(quantity, t);
    }

    public long size() {
        return size.get();
    }
}
