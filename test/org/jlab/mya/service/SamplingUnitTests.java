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
import org.jlab.mya.params.BasicSamplerParams;
import org.jlab.mya.params.NaiveSamplerParams;
import org.jlab.mya.stream.FloatEventStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;

/**
 * Unit tests for sampling service.
 *
 * @author slominskir
 */
public class SamplingUnitTests {

    private SamplingService service;

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        DataNexus nexus = new OnDemandNexus(Deployment.ops);
        service = new SamplingService(nexus);
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

        Metadata metadata = service.findMetadata(pv);
        NaiveSamplerParams params = new NaiveSamplerParams(metadata, begin,
                end, limit);            
        
        long expSize = 21; // We limit to 24, but we know historical data only has 21
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = service.openNaiveSamplerFloatStream(params)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                System.out.println(event.toString(fractionalDigits));
            }
        }
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
        
        Metadata metadata = service.findMetadata(pv);
        BasicSamplerParams params = new BasicSamplerParams(metadata, begin,
                stepMilliseconds, sampleCount);  

        long expSize = 24;
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = service.openBasicSamplerFloatStream(params)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                System.out.println(event);
            }
        }
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }
}
