package org.jlab.mya.service;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Metadata;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.*;
import org.jlab.mya.stream.FloatEventStream;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;

/**
 * Unit tests for sampling sampleService.
 *
 * @author slominskir
 */
public class SamplingServiceTest {

    private SamplingService sampleService;
    private IntervalService intervalService;

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        DataNexus nexus = new OnDemandNexus("history");
        sampleService = new SamplingService(nexus);
        intervalService = new IntervalService(nexus);
    }

    @After
    public void tearDown() throws SQLException {

    }

    /**
     * Test naive sampler.
     *
     * Compare with "myget -l 24 -c R123PMES -b 2017-01-01 -e 2017-01-25 -f 6"
     */
    @Test
    public void testBinnedSampler() throws Exception {
        
        String pv = "R123PMES";
        Instant begin = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-01-25T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();        
        long limit = 24;
        int fractionalDigits = 6; // microseconds; seems to be max precision of myget

        Metadata metadata = sampleService.findMetadata(pv);
        BinnedSamplerParams params = new BinnedSamplerParams(metadata, begin,
                end, limit);

        long expSize = 24; // We limit to 24, but we know historical data only has 21
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openBinnedSamplerFloatStream(params)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
        //        System.out.println(event.toString(fractionalDigits));
            }
        }
        assertEquals(expSize, eventList.size());
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }

    /**
     * Test basic sampler.
     *
     * Compare with: "mySampler -b 2017-01-01 -s 1d -n 24 R123PMES"
     */
    @Test
    public void testBasicSampler() throws Exception {

        String pv = "R123PMES";
        Instant begin = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        long stepMilliseconds = 86400000;
        long sampleCount = 24;        
        
        Metadata metadata = sampleService.findMetadata(pv);
        BasicSamplerParams params = new BasicSamplerParams(metadata, begin,
                stepMilliseconds, sampleCount);  

        long expSize = 24;
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openBasicSamplerFloatStream(params)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
        //        System.out.println(event);
            }
        }
        assertEquals("List size does not match expected", expSize, eventList.size());
    }
    
    /**
     * Test improved sampler.
     */
    @Test
    public void testEventSampler() throws Exception {
        
        String pv = "R123PMES";
        // 20 second test
        Instant begin = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-01-01T00:00:20").atZone(
                ZoneId.systemDefault()).toInstant();        

        // One year test
//        Instant begin = LocalDateTime.parse("2016-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
//        Instant end = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();   
        long limit = 10;
        int displayFractionalDigits = 6; // microseconds; seems to be max precision of myget

        Metadata metadata = sampleService.findMetadata(pv);
        
        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);
        
        long count = intervalService.count(params);
        
        //System.out.println("count: " + count);
        
        EventSamplerParams samplerParams = new EventSamplerParams(metadata, begin,
                end, limit, count);            
        
        long expSize = 10; // Not sure it will always be exact, might be +/- 1 in some combinations of count and limit
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openEventSamplerFloatStream(samplerParams)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
//                System.out.println(event.toString(displayFractionalDigits));
            }
        }
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }    

    /**
     * Test advanced sampler.
     */
    @Test
    public void testGraphicalSampler() throws Exception {

        // Limited test        
        String pv = "R12XGMES";
        Instant begin = LocalDateTime.parse("2017-02-11T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-02-11T02:30:00").atZone(
                ZoneId.systemDefault()).toInstant();

//        String pv = "R123PMES";
//        Instant begin = LocalDateTime.parse("2016-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
//        Instant end = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();        
        long numBins = 10;
        int displayFractionalDigits = 6; // microseconds; seems to be max precision of myget

        Metadata metadata = sampleService.findMetadata(pv);
        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);
        long count = intervalService.count(params);
        
        GraphicalSamplerParams samplerParams = new GraphicalSamplerParams(metadata, begin,
                end, numBins, count);

        // Impossible to know how many data points will be generated a priori since every disconnect will be represented.
        // The expected size gets complicated to predict.  Number of actual bins is a complicated calculation based on determining
        // the smallest bin size that produces no more than numBins-2.  Then each bin can produce min, max, and largest triangle three bucket (lttb) point
        // and also return an unlimited number of non-update events plus surrounding update events.  A good rule of thumb for number of points returned is
        // between 1*(numBins-2) + numNonUpdateEvents*3 + 2 and 3*(numBins-2) + numNonUpdateEvents*3 + 2
        long expSize = 15; // 2 + 1*8 = 10, 2 + 3*8 = 26, so it's a broad range

        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openGraphicalSamplerFloatStream(samplerParams)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                System.out.println("##READ :" + event.toString(displayFractionalDigits));
            }
            System.out.println("Downsampled num: " + eventList.size());
            System.out.println("Max Exepected update num: " + ( (numBins-2)*3 + 2) );
        }

        // Since we know ahead of time there are 20 points of data, start + end + (10 bins with min,max,lttb points) + zero non-update events = between 12 and 20
        assertEquals("List size does not match expected", expSize, eventList.size());
    }    
}
