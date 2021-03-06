package org.jlab.mya;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.EventStream;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Unit tests for SourceSamplingService.  Sampling done in-database.   See also: stream.wrapped.ApplicationLevelSamplingTest.
 *
 * @author slominskir
 */
public class SourceSamplingServiceTest {

    private DataNexus nexus;

    @Before
    public void setUp() {
        nexus = new OnDemandNexus("history");
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

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        long expSize = 24; // We limit to 24, but we know historical data only has 21
        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = nexus.openMyGetSampleStream(metadata, begin, end, limit)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                //        System.out.println(event.toString(fractionalDigits));
            }
        }
        Assert.assertEquals(expSize, eventList.size());
        if (eventList.size() != expSize) {
            Assert.fail("List size does not match expected");
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

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        long expSize = 24;
        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = nexus.openMySamplerStream(metadata, begin, stepMilliseconds, sampleCount)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                //        System.out.println(event);
            }
        }
        Assert.assertEquals("List size does not match expected", expSize, eventList.size());
    }
}
