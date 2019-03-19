package org.streaminer.stream.frequency;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.mahout.math.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import org.streaminer.stream.frequency.util.CountEntry;
import org.streaminer.util.hash.HashUtils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CCFCSketchTest {
    
    public CCFCSketchTest() {
    }

    /**
     * Test of add method, of class CCFCSketch.
     */
    @Test
    public void testFrequency() {
        int n=1048575, lgn=10, range=123456;
        int width = 512, depth = 5, gran = 1;
        double phi = 0.01;
        
        StreamGenerator gen = new StreamGenerator(1.1, n, range);
        gen.generate();
        
        lgn = 20;
        
        double thresh = Math.floor(phi*(double)range);  
        if (thresh == 0) thresh = 1.0;
        
        int hh = gen.exact((int) thresh);
        long[] stream = gen.stream;
        
        CCFCSketch sketch = new CCFCSketch(width, depth, lgn, gran);
        for (int i=1; i<=range; i++) 
            //if (stream[i]>0)
                sketch.add((int)stream[i], 1);      
           // else
           //     sketch.add((int)-stream[i], -1);   
        
        // actual frequency
        RealCounting<Integer> actualFreq = new RealCounting<Integer>();
        for (int i=1; i<=range; i++)
            actualFreq.add((int)stream[i], 1);      
        
        List<CountEntry<Integer>> topk = actualFreq.peek(10);
        
        System.out.println("Frequency Table\n" + StringUtils.repeat("-", 80));
        System.out.println("Item\tactual\testimated");
        for (CountEntry<Integer> item : topk) {
            System.out.println(item.getItem() + "\t" 
                    + item.getFrequency() + "\t" 
                    + sketch.estimateCount(item.getItem(), 0));
        }
        /*
        int[] outlist = sketch.output((int) thresh);
        
        System.out.println("Output: " + Arrays.toString(outlist));
        
        int val = 96699;
        System.out.println("Estimative for " + val + ": " + sketch.estimateCount(96699, depth));
        
        gen.checkOutput(outlist, (int) thresh, hh);*/
    }
    
    
    @Test
    public void testF2Estimative() {
        int n = 1048575;
        int range = 12345;
        
        CCFCSketch sketch = new CCFCSketch(512,5,1,1);
        StreamGenerator gen = new StreamGenerator(0.8, n, range);
        gen.generate();
        gen.exact();
        
        long[] stream = gen.stream;
        long sumsq = gen.sumsq;
        
        for (int i=1; i<=range; i++) 
            sketch.add((int)stream[i], 1);  
        
        System.out.println("Exact F2: " + sumsq);
        System.out.println("Estimated F2: " + sketch.estimateF2());
    }
    
}