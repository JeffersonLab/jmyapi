package org.jlab.mya;

import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

import org.jlab.mya.stream.BoundaryAwareStream;
import org.jlab.mya.stream.EventStream;
import org.jlab.mya.stream.FloatAnalysisStream;
import org.jlab.mya.stream.FloatGraphicalSampleStream;
import org.junit.Test;

/**
 *
 * @author slominskir
 */
public class PerformanceTest {

    @Test
    public void doNestedStreamsTest() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus("history");

        String pv = "IDC1G03sh_cur";
        Instant begin = TimeUtil.toLocalDT("2016-06-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2017-07-01T00:00:00");

        Runtime rt = Runtime.getRuntime();
        long stopBytes;
        long startMillis;
        long stopMillis;

        System.out.println("---- Metadata Query ----");
        rt.gc();
        startMillis = System.currentTimeMillis();

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));

        System.out.println("---- Event Count Query ----");
        rt.gc();
        startMillis = System.currentTimeMillis();
        long count = nexus.count(metadata, begin, end);
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));
        System.out.println("Event count: " + String.format("%,d", count));

        System.out.println("---- Event Interval Query ----");
        rt.gc();
        startMillis = System.currentTimeMillis();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                //System.out.println(event);
            }
        }
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));

        System.out.println("---- Prior Point Lookup ----");
        rt.gc();
        startMillis = System.currentTimeMillis();
        FloatEvent priorPoint = nexus.findEvent(metadata, begin);
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));

        System.out.println("---- BoundaryAwareStream ----");
        rt.gc();
        startMillis = System.currentTimeMillis();

        try (
                final EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end);
                final BoundaryAwareStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class);
        ) {

            FloatEvent event;

            while ((event = boundaryStream.read()) != null) {
                //System.out.println(event);
            }
        }

        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));




        System.out.println("---- FloatAnalysisStream ----");
        rt.gc();
        startMillis = System.currentTimeMillis();

        try (
                final EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end);
                final FloatAnalysisStream analysisStream = new FloatAnalysisStream(stream, new short[]{RunningStatistics.INTEGRATION});
        ) {

            FloatEvent event;

            while ((event = analysisStream.read()) != null) {
                //System.out.println(event);
            }
        }

        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));


        System.out.println("---- GraphicalSamplerStream ----");
        rt.gc();
        startMillis = System.currentTimeMillis();

        try (
                final EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end);
                final FloatGraphicalSampleStream<FloatEvent> samplerStream = new FloatGraphicalSampleStream<>(stream, 3, count, FloatEvent.class);
        ) {

            FloatEvent event;

            while ((event = samplerStream.read()) != null) {
                //System.out.println(event);
            }
        }

        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));
    }

    /**
     * Looks like streaming is nearly always best if count > 16,000.
     * If less than perhaps 4096 then transferring results all
     * at once back is generally slightly faster.
     * If users wanted to be clever and they knew result size (do a count query
     * first) then they could choose a strategy dynamically.  Likely this is unnecessary though since streaming (the
     * default in jmyapi)  is always the best in terms of minimal memory usage and is almost as good with small datasets
     * in terms of speed as other options and actually better than rest with large datasets presumably since users
     * can begin processing the data nearly immediately (less latency) instead of waiting for a big chunk (or all of it)
     * to arrive.
     *
     * @throws SQLException If unable to query the SQL database
     * @throws IOException If unable to stream data
     */
    @Test
    public void doIntervalFetchStrategyTest() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus("history");

        String pv = "IDC1G03sh_cur";
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2018-01-26T18:00:00");

        Runtime rt = Runtime.getRuntime();
        long startBytes;
        long stopBytes;
        long startMillis;
        long stopMillis;

        System.out.println("---- Metadata Query ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));

        System.out.println("---- Event Count Query ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        long count = nexus.count(metadata, begin, end);
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));
        System.out.println("Event count: " + String.format("%,d", count));

        System.out.println("---- Interval Query: Streaming Strategy ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                //System.out.println(event);
            }
        }
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));


        System.out.println("---- Interval Query: Chunk Strategy ----");

        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end, DataNexus.IntervalQueryFetchStrategy.CHUNK, false)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                //System.out.println(event);
            }
        }
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));


        System.out.println("---- Interval Query: ALL Strategy ----");

        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end, DataNexus.IntervalQueryFetchStrategy.ALL, false)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                //System.out.println(event);
            }
        }
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));
    }
}
