package org.jlab.mya.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.jlab.mya.DataNexus;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.MyGetSampleParams;
import org.jlab.mya.params.MySamplerParams;
import org.jlab.mya.stream.FloatEventStream;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Unit tests for SourceSamplingService.  Sampling done in-database.   See also: stream.wrapped.ApplicationLevelSamplingTest.
 *
 * @author slominskir
 */
public class SourceSamplingServiceTest {

    private SourceSamplingService sampleService;

    @Before
    public void setUp() {
        DataNexus nexus = new OnDemandNexus("history");
        sampleService = new SourceSamplingService(nexus);
    }

    /**
     * Test naive sampler.
     * <p>
     * Compare with "myget -l 24 -c R123PMES -b 2017-01-01 -e 2017-01-25 -f 6"
     */
    @Test
    public void testMyGetSampler() throws Exception {

        String pv = "R123PMES";
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2017-01-25T00:00:00");
        long limit = 24;
        int fractionalDigits = 6; // microseconds; seems to be max precision of myget

        Metadata metadata = sampleService.findMetadata(pv);
        MyGetSampleParams params = new MyGetSampleParams(metadata, begin,
                end, limit);

        long expSize = 24; // We limit to 24, but we know historical data only has 21
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openMyGetSampleFloatStream(params)) {
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
     * <p>
     * Compare with: "mySampler -b 2017-01-01 -s 1d -n 24 R123PMES"
     */
    @Test
    public void testMySampler() throws Exception {

        String pv = "R123PMES";
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00");
        long stepMilliseconds = 86400000;
        long sampleCount = 24;

        Metadata metadata = sampleService.findMetadata(pv);
        MySamplerParams params = new MySamplerParams(metadata, begin,
                stepMilliseconds, sampleCount);

        long expSize = 24;
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = sampleService.openMySamplerFloatStream(params)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                //        System.out.println(event);
            }
        }
        assertEquals("List size does not match expected", expSize, eventList.size());
    }
}
