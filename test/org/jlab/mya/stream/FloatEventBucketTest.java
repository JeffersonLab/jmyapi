/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.mya.stream;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.jlab.mya.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author adamc
 */
public class FloatEventBucketTest {
    
    public FloatEventBucketTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of downSample and getDownSample method, of class FloatEventBucket.
     */
    @Test
    public void testDownSampleAndGetDownSample() {
        System.out.println("downSample");
        Instant begin = LocalDateTime.parse("2017-03-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-03-02T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();

        FloatEvent e1 = new FloatEvent(begin.minusSeconds(1), EventCode.UPDATE, 1.0f);  // just before bucket
        FloatEvent e3 = new FloatEvent(end.plusSeconds(1), EventCode.UPDATE, 0.0f); // just after bucket

        List<FloatEvent> events = new ArrayList<>();
        
        // Event adds start on line 70 so they are easier to count
        events.add(new FloatEvent(begin.plusSeconds(1),        EventCode.NETWORK_DISCONNECTION, 0.0f ));  // non-update
        events.add(new FloatEvent(begin.plusSeconds(2),        EventCode.NETWORK_DISCONNECTION, 0.0f ));  // non-update
        events.add(new FloatEvent(begin.plusSeconds(3),        EventCode.NETWORK_DISCONNECTION, 0.0f ));  // non-update
        events.add(new FloatEvent(begin.plusSeconds(5),        EventCode.UPDATE, 10.0f ));  // max
        events.add(new FloatEvent(begin.plusSeconds(3998),   EventCode.UPDATE, 0.9f ));  // Should be skipped
        events.add(new FloatEvent(begin.plusSeconds(3999),   EventCode.UPDATE, 1.9f ));  // Included becuase it preceds a non-update
        events.add(new FloatEvent(begin.plusSeconds(4000),   EventCode.NETWORK_DISCONNECTION, 0.0f));  // non-update
        events.add(new FloatEvent(begin.plusSeconds(5000),   EventCode.UPDATE, 2.0f ));  // Included since it follows a non-update event
        events.add(new FloatEvent(begin.plusSeconds(43200), EventCode.UPDATE, 9.99f ));  // lttb with given e1, e3
        events.add(new FloatEvent(begin.plusSeconds(43201), EventCode.UPDATE, 0.9f ));  // Should be skipped
        events.add(new FloatEvent(begin.plusSeconds(43202), EventCode.UPDATE, 1.9f ));  // Should be skipped
        events.add(new FloatEvent(begin.plusSeconds(43203), EventCode.UPDATE, 9.9f ));  // Included since it precedes a non-update event
        events.add(new FloatEvent(begin.plusSeconds(47200), EventCode.NETWORK_DISCONNECTION, 0.0f));  // non-update
        events.add(new FloatEvent(begin.plusSeconds(50000), EventCode.UPDATE, -2.1f ));  // Included since it follows a non-update event
        events.add(new FloatEvent(end.minusSeconds(5),        EventCode.UPDATE, 3.3f ));  // should be skipped
        events.add(new FloatEvent(end.minusSeconds(1),        EventCode.UPDATE, -5.1f)); // min

        FloatEventBucket instance = new FloatEventBucket(events);
        FloatEvent expLTTB = events.get(8);
        FloatEvent resultLTTB = instance.downSample(e1, e3);
        SortedSet<FloatEvent> expSet = new TreeSet<>();
        // Added the lists
        int[] keepers = {0,1,2,3,5,6,7,8,11,12,13,15};
        for(int i : keepers ) {
            expSet.add(events.get(i));
        }
        SortedSet<FloatEvent> resultSet = instance.getDownSampledOutput();
        assertEquals(expLTTB, resultLTTB);
        assertEquals(expSet, resultSet);
    }

    /**
     * Test of calculateTriangleArea method, of class FloatEventBucket.
     */
    @Test
    public void testCalculateTriangleArea() {
        System.out.println("calculateTriangleArea");
        List<FloatEvent> events = new ArrayList<>();
        Instant now = Instant.now();
        
        // Triangle 1 - area = 5
        events.add(new FloatEvent(now, EventCode.UPDATE, 0.0f));
        events.add(new FloatEvent(now.minusSeconds(5), EventCode.UPDATE, 1.0f));
        events.add(new FloatEvent(now.minusSeconds(10), EventCode.UPDATE, 0.0f));

        // Triangle 2 - should be identical to triangle 1
        events.add(new FloatEvent(now.minusSeconds(10000), EventCode.UPDATE, 0.0f));
        events.add(new FloatEvent(now.minusSeconds(10005), EventCode.UPDATE, 1.0f));
        events.add(new FloatEvent(now.minusSeconds(10010), EventCode.UPDATE, 0.0f));
        
        // Triangle 3 - 
        events.add(new FloatEvent(now.minusSeconds(1000000000), EventCode.UPDATE, 50.0f));
        events.add(new FloatEvent(now.minusSeconds(1050000000), EventCode.UPDATE, 1.0f));
        events.add(new FloatEvent(now.minusSeconds(1100000000), EventCode.UPDATE, 0.0f));

        double expResult1 = 5;
        double expResult2 = 5;
        double expResult3 = 1200000000;
        double result1 = FloatEventBucket.calculateTriangleArea(events.get(0), events.get(1), events.get(2));
        double result2 = FloatEventBucket.calculateTriangleArea(events.get(3), events.get(4), events.get(5));
        double result3 = FloatEventBucket.calculateTriangleArea(events.get(6), events.get(7), events.get(8));
        
        // Not sure how much rounding error will occur - that should be plenty close enough.
        assertEquals(expResult1, result1, 0.00000001);
        assertEquals(expResult2, result2, 0.00000001);
        assertEquals(expResult3, result3, 0.00000001);
    }

}
