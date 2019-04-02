package org.jlab.mya.service;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
import org.jlab.mya.Metadata;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.AdvancedSamplerParams;
import org.jlab.mya.params.BasicSamplerParams;
import org.jlab.mya.params.ImprovedSamplerParams;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.NaiveSamplerParams;
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
        DataNexus nexus = new OnDemandNexus(Deployment.history);
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
    public void testNaiveSampler() throws Exception {
        
        String pv = "R123PMES";
        Instant begin = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-01-25T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();        
        long limit = 24;
        int fractionalDigits = 6; // microseconds; seems to be max precision of myget

        Metadata metadata = sampleService.findMetadata(pv);
        NaiveSamplerParams params = new NaiveSamplerParams(metadata, begin,
                end, limit);            
        
        long expSize = 24; // We limit to 24, but we know historical data only has 21
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openNaiveSamplerFloatStream(params)) {
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
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }
    
    /**
     * Test improved sampler.
     */
    @Test
    public void testImprovedSampler() throws Exception {
        
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
        
        ImprovedSamplerParams samplerParams = new ImprovedSamplerParams(metadata, begin,
                end, limit, count);            
        
        long expSize = 10; // Not sure it will always be exact, might be +/- 1 in some combinations of count and limit
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openImprovedSamplerFloatStream(samplerParams)) {
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
    public void testAdvancedSampler() throws Exception {

        // Limited test        
        String pv = "R12XGMES";
        Instant begin = LocalDateTime.parse("2017-02-11T02:45:23").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-02-11T05:00:36").atZone(
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
        
        AdvancedSamplerParams samplerParams = new AdvancedSamplerParams(metadata, begin,
                end, numBins, count);

        long expSize = 10; // Not sure it will always be exact, might be +/- 1 in some combinations of count and limit

        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openAdvancedSamplerFloatStream(samplerParams)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                System.out.println("##READ :" + event.toString(displayFractionalDigits));
            }
            System.out.println("Downsampled num: " + eventList.size());
            System.out.println("Max Exepected update num: " + ( (numBins-2)*3 + 2) );
        }
//        if (eventList.size() != expSize) {
//            fail("List size does not match expected");
//        }
    }    
}
