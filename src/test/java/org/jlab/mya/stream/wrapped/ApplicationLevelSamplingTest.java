package org.jlab.mya.stream.wrapped;

import org.jlab.mya.*;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.AnalyzedFloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.GraphicalEventBinSamplerParams;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.params.SimpleEventBinSamplerParams;
import org.jlab.mya.service.IntervalService;
import org.jlab.mya.service.PointService;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.ListStream;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ApplicationLevelSamplingTest {

    private IntervalService intervalService;
    private PointService pointService;

    @Before
    public void setUp() {
        DataNexus nexus = new OnDemandNexus("history");
        intervalService = new IntervalService(nexus);
        pointService = new PointService(nexus);
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

        Metadata<FloatEvent> metadata = intervalService.findMetadata(pv, FloatEvent.class);

        IntervalQueryParams<FloatEvent> params = new IntervalQueryParams<>(metadata, begin, end);

        long count = intervalService.count(params);

        //System.out.println("count: " + count);

        SimpleEventBinSamplerParams samplerParams = new SimpleEventBinSamplerParams(limit, count);

        long expSize = 10; // Not sure it will always be exact, might be +/- 1 in some combinations of count and limit
        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = intervalService.openEventStream(params)) {
            FloatSimpleEventBinSampleStream<FloatEvent> sampleStream = new FloatSimpleEventBinSampleStream<>(stream, samplerParams, FloatEvent.class);
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

        Metadata<FloatEvent> metadata = intervalService.findMetadata(pv, FloatEvent.class);

        long stop = System.currentTimeMillis();

        System.out.println("Metadata lookup Took: " + (stop - start) / 1000.0 + " seconds");

        IntervalQueryParams<FloatEvent> params = new IntervalQueryParams<>(metadata, begin, end);

        start = System.currentTimeMillis();

        long count = intervalService.count(params);

        stop = System.currentTimeMillis();

        System.out.println("Count Took: " + (stop - start) / 1000.0 + " seconds");

        start = System.currentTimeMillis();

        PointQueryParams<FloatEvent> pointParams = new PointQueryParams<>(metadata, begin);
        FloatEvent priorPoint = pointService.findFloatEvent(pointParams);

        stop = System.currentTimeMillis();

        System.out.println("Prior Point lookup Took: " + (stop - start) / 1000.0 + " seconds");

        System.out.println("count (excluding boundaries): " + count);

        SimpleEventBinSamplerParams samplerParams = new SimpleEventBinSamplerParams(limit, count);

        short[] eventStatsMap = new short[]{RunningStatistics.INTEGRATION};

        long expSize = 12;
        List<FloatEvent> eventList = new ArrayList<>();

        start = System.currentTimeMillis();
        try (
                final EventStream<FloatEvent> stream = intervalService.openEventStream(params);
                final BoundaryAwareStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class);
                final FloatAnalysisStream analysisStream = new FloatAnalysisStream(boundaryStream, eventStatsMap);
                final FloatSimpleEventBinSampleStream<AnalyzedFloatEvent> sampleStream = new FloatSimpleEventBinSampleStream<>(analysisStream, samplerParams, AnalyzedFloatEvent.class);
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

        Metadata<FloatEvent> metadata = intervalService.findMetadata(pv, FloatEvent.class);
        IntervalQueryParams<FloatEvent> params = new IntervalQueryParams<>(metadata, begin, end);
        long count = intervalService.count(params);

        System.out.println("count: " + count);

        GraphicalEventBinSamplerParams samplerParams = new GraphicalEventBinSamplerParams(numBins, count);

        // Impossible to know how many data points will be generated a priori since every disconnect will be represented.
        // The expected size gets complicated to predict.  Number of actual bins is a complicated calculation based on determining
        // the smallest bin size that produces no more than numBins-2.  Then each bin can produce min, max, and largest triangle three bucket (lttb) point
        // and also return an unlimited number of non-update events plus surrounding update events.  A good rule of thumb for number of points returned is
        // between 1*(numBins-2) + numNonUpdateEvents*3 + 2 and 3*(numBins-2) + numNonUpdateEvents*3 + 2
        long expSize = 15; // 2 + 1*8 = 10, 2 + 3*8 = 26, so it's a broad range

        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = intervalService.openEventStream(params)) {
            try (FloatGraphicalEventBinSampleStream<FloatEvent> sampleStream = new FloatGraphicalEventBinSampleStream<>(stream, samplerParams, FloatEvent.class)) {
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

        GraphicalEventBinSamplerParams samplerParams = new GraphicalEventBinSamplerParams(numBins, count);

        long expSize = 10;

        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
            try (FloatGraphicalEventBinSampleStream<FloatEvent> sampleStream = new FloatGraphicalEventBinSampleStream<>(stream, samplerParams, FloatEvent.class)) {
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
