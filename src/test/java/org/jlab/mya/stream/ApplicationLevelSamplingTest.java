package org.jlab.mya.stream;

import org.jlab.mya.*;
import org.jlab.mya.RunningStatistics;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.AnalyzedFloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ApplicationLevelSamplingTest {

    private DataNexus nexus;

    @Before
    public void setUp() {
        nexus = new OnDemandNexus("history");
    }

    /**
     * Test simple application layer event bin sampler.
     */
    @Test
    public void testSimpleEventBinSampler() throws Exception {

        String pv = "R123PMES";
        // 20 second test
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2017-01-01T00:00:20");

        // One year test
//        Instant begin = TimeUtil.toLocalDT("2016-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
//        Instant end = TimeUtil.toLocalDT("2017-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
        long limit = 10;

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        long count = nexus.count(metadata, begin, end);

        //System.out.println("count: " + count);

        long expSize = 10; // Not sure it will always be exact, might be +/- 1 in some combinations of count and limit
        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {
            FloatSimpleSampleStream<FloatEvent> sampleStream = new FloatSimpleSampleStream<>(stream, limit, count, FloatEvent.class);
            FloatEvent event;
            while ((event = sampleStream.read()) != null) {
                eventList.add(event);
//                System.out.println(event.toString(displayFractionalDigits));
            }
        }
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }

    /**
     * Test integrate stream then simple application layer event bin sampler.
     */
    @Test
    public void testAnalyzeThenSimpleEventBinSampler() throws Exception {

        String pv = "R123PMES";
        // 20 second test
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2017-01-01T00:00:20");

        // One year test
//        Instant begin = TimeUtil.toLocalDT("2016-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
//        Instant end = TimeUtil.toLocalDT("2017-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
        long limit = 10;

        long start = System.currentTimeMillis();

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        long stop = System.currentTimeMillis();

        System.out.println("Metadata lookup Took: " + (stop - start) / 1000.0 + " seconds");

        start = System.currentTimeMillis();

        long count = nexus.count(metadata, begin, end);

        stop = System.currentTimeMillis();

        System.out.println("Count Took: " + (stop - start) / 1000.0 + " seconds");

        start = System.currentTimeMillis();

        FloatEvent priorPoint = nexus.findEvent(metadata, begin);

        stop = System.currentTimeMillis();

        System.out.println("Prior Point lookup Took: " + (stop - start) / 1000.0 + " seconds");

        System.out.println("count (excluding boundaries): " + count);

        short[] eventStatsMap = new short[]{RunningStatistics.INTEGRATION};

        long expSize = 12;
        List<FloatEvent> eventList = new ArrayList<>();

        start = System.currentTimeMillis();
        try (
                final EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end);
                final BoundaryAwareStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class);
                final FloatAnalysisStream analysisStream = new FloatAnalysisStream(boundaryStream, eventStatsMap);
                final FloatSimpleSampleStream<AnalyzedFloatEvent> sampleStream = new FloatSimpleSampleStream<>(analysisStream, limit, count, AnalyzedFloatEvent.class);
        ) {
            AnalyzedFloatEvent event;

            while ((event = sampleStream.read()) != null) {
                System.out.println(event.toString(2) + ", integration: " + event.getEventStats()[0]);
                eventList.add(event);
            }

            RunningStatistics stats = analysisStream.getLatestStats();

            System.out.println("Max: " + stats.getMax());
            System.out.println("Min: " + stats.getMin());
            System.out.println("Mean: " + stats.getMean());

            if (eventList.size() != expSize) {
                fail("List size does not match expected");
            }
        }

        stop = System.currentTimeMillis();

        System.out.println("Stream Section Took: " + (stop - start) / 1000.0 + " seconds");
    }

    /**
     * Test advanced application level event bin sampler.
     */
    @Test
    public void testGraphicalSampler() throws Exception {

        // Limited test
        String pv = "R12XGMES";
        Instant begin = TimeUtil.toLocalDT("2017-02-11T00:00:00");
        Instant end = TimeUtil.toLocalDT("2017-02-11T02:30:00");

//        String pv = "R123PMES";
//        Instant begin = TimeUtil.toLocalDT("2016-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
//        Instant end = TimeUtil.toLocalDT("2017-01-01T00:00:00").atZone(
//                ZoneId.systemDefault()).toInstant();
        long numBins = 10;
        int displayFractionalDigits = 6; // microseconds; seems to be max precision of myget

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
        long count = nexus.count(metadata, begin, end);

        System.out.println("count: " + count);

        // Impossible to know how many data points will be generated a priori since every disconnect will be represented.
        // The expected size gets complicated to predict.  Number of actual bins is a complicated calculation based on determining
        // the smallest bin size that produces no more than numBins-2.  Then each bin can produce min, max, and largest triangle three bucket (lttb) point
        // and also return an unlimited number of non-update events plus surrounding update events.  A good rule of thumb for number of points returned is
        // between 1*(numBins-2) + numNonUpdateEvents*3 + 2 and 3*(numBins-2) + numNonUpdateEvents*3 + 2
        long expSize = 15; // 2 + 1*8 = 10, 2 + 3*8 = 26, so it's a broad range

        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {
            try (FloatGraphicalSampleStream<FloatEvent> sampleStream = new FloatGraphicalSampleStream<>(stream, numBins, count, FloatEvent.class)) {
                FloatEvent event;
                while ((event = sampleStream.read()) != null) {
                    eventList.add(event);
                    System.out.println("##READ :" + event.toString(displayFractionalDigits));
                }
                System.out.println("Downsampled num: " + eventList.size());
                System.out.println("Max Expected update num: " + ((numBins - 2) * 3 + 2));
            }
        }

        // Since we know ahead of time there are 20 points of data, start + end + (10 bins with min,max,lttb points) + zero non-update events = between 12 and 20
        assertEquals("List size does not match expected", expSize, eventList.size());
    }

    /**
     * Test advanced application level event bin sampler around non-update events.
     */
    @Test
    public void testGraphicalSamplerNonUpdateEvents() throws Exception {
        long numBins = 30;
        int displayFractionalDigits = 6; // microseconds; seems to be max precision of myget

        List<FloatEvent> events = new ArrayList<>();
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 1));
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 2));
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 3));
        // bucket break for three buckets
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 4));
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 5));
        events.add(new FloatEvent(Instant.now(), EventCode.NETWORK_DISCONNECTION, 0));
        // bucket break for three buckets
        events.add(new FloatEvent(Instant.now(), EventCode.NETWORK_DISCONNECTION, 0));
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 6));
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 7));
        events.add(new FloatEvent(Instant.now(), EventCode.UPDATE, 8));

        long count = events.size();

        System.out.println("count: " + count);

        long expSize = 10;

        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
            try (FloatGraphicalSampleStream<FloatEvent> sampleStream = new FloatGraphicalSampleStream<>(stream, numBins, count, FloatEvent.class)) {
                FloatEvent event;
                while ((event = sampleStream.read()) != null) {
                    eventList.add(event);
                    System.out.println("##READ :" + event.toString(displayFractionalDigits));
                }
                System.out.println("Downsampled num: " + eventList.size());
                System.out.println("Max Expected update num: " + ((numBins - 2) * 3 + 2));
            }
        }

        // Since we know ahead of time there are 20 points of data, start + end + (10 bins with min,max,lttb points) + zero non-update events = between 12 and 20
        assertEquals("List size does not match expected", expSize, eventList.size());
    }
}
