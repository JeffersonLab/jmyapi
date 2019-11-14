package org.jlab.mya.stream.wrapped;

import org.jlab.mya.DataNexus;
import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.Metadata;
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

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

        Metadata metadata = intervalService.findMetadata(pv);

        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);

        long count = intervalService.count(params);

        //System.out.println("count: " + count);

        SimpleEventBinSamplerParams samplerParams = new SimpleEventBinSamplerParams(limit, count);

        long expSize = 10; // Not sure it will always be exact, might be +/- 1 in some combinations of count and limit
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = intervalService.openFloatStream(params)) {
            FloatSimpleEventBinSampleStream sampleStream = new FloatSimpleEventBinSampleStream(stream, samplerParams);
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

        Metadata metadata = intervalService.findMetadata(pv);

        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);

        long count = intervalService.count(params);

        PointQueryParams pointParams = new PointQueryParams(metadata, begin);
        FloatEvent priorPoint = pointService.findFloatEvent(pointParams);

        System.out.println("count (excluding boundaries): " + count);

        SimpleEventBinSamplerParams samplerParams = new SimpleEventBinSamplerParams(limit, count);

        short[] eventStatsMap = new short[]{RunningStatistics.INTEGRATION};

        long expSize = 12;
        List<FloatEvent> eventList = new ArrayList<>();

        try (FloatEventStream stream = intervalService.openFloatStream(params)) {
            try(BoundaryAwareStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false)) {
                try (FloatAnalysisStream analysisStream = new FloatAnalysisStream(boundaryStream, eventStatsMap)) {
                    try (FloatSimpleEventBinSampleStream<AnalyzedFloatEvent> sampleStream = new FloatSimpleEventBinSampleStream<>(analysisStream, samplerParams)) {
                        AnalyzedFloatEvent event;
                        while ((event = sampleStream.read()) != null) {
                            System.out.println(event.toString(2) + ", integration: " + event.getEventStats()[0]);
                            eventList.add(event);
                        }
                    }

                    RunningStatistics stats = analysisStream.getLatestStats();

                    System.out.println("Max: " + stats.getMax());
                    System.out.println("Min: " + stats.getMin());
                    System.out.println("Mean: " + stats.getMean());
                }
            }

            if (eventList.size() != expSize) {
                fail("List size does not match expected");
            }
        }
    }

    /**
     * Test advanced application level event bin sampler.
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

        Metadata metadata = intervalService.findMetadata(pv);
        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);
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
        try (FloatEventStream stream = intervalService.openFloatStream(params)) {
            try (FloatGraphicalEventBinSampleStream sampleStream = new FloatGraphicalEventBinSampleStream(stream, samplerParams)) {
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
        String pv = "R123PMES";
        Instant begin = LocalDateTime.parse("2019-05-30T09:07:17").atZone( /*Network disconnection time*/
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2019-05-30T09:56:50").atZone(
                ZoneId.systemDefault()).toInstant(); /*Network disconnection time*/

        long numBins = 30;
        int displayFractionalDigits = 6; // microseconds; seems to be max precision of myget

        List<FloatEvent> events = new ArrayList<FloatEvent>();
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
        try (EventStream<FloatEvent> stream = new ListStream<FloatEvent>(events)) {
            try (FloatGraphicalEventBinSampleStream sampleStream = new FloatGraphicalEventBinSampleStream(stream, samplerParams)) {
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