package org.jlab.mya;

import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.GraphicalEventBinSamplerParams;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.service.IntervalService;
import org.jlab.mya.service.PointService;
import org.jlab.mya.stream.FloatEventStream;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

import org.jlab.mya.stream.wrapped.BoundaryAwareStream;
import org.jlab.mya.stream.wrapped.FloatAnalysisStream;
import org.jlab.mya.stream.wrapped.FloatGraphicalEventBinSampleStream;
import org.junit.Test;

/**
 *
 * @author slominskir
 */
public class PerformanceTest {

    @Test
    public void doTest() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus("history");
        IntervalService service = new IntervalService(nexus);
        PointService pointService = new PointService(nexus);

        String pv = "IDC1G03sh_cur";
        Instant begin
                = TimeUtil.toLocalDT("2016-06-01T00:00:00");
        Instant end
                = TimeUtil.toLocalDT("2017-07-01T00:00:00");

        Runtime rt = Runtime.getRuntime();
        long startBytes;
        long stopBytes;
        long startMillis;
        long stopMillis;

        System.out.println("---- Metadata Query ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();

        Metadata metadata = service.findMetadata(pv);

        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));

        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);

        System.out.println("---- Event Count Query ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        long count = service.count(params);
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));
        System.out.println("Event count: " + String.format("%,d", count));

        System.out.println("---- Event Interval Query ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        try (FloatEventStream stream = service.openFloatStream(params)) {

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
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();
        PointQueryParams pointParams = new PointQueryParams(metadata, begin);
        FloatEvent priorPoint = pointService.findFloatEvent(pointParams);
        stopMillis = System.currentTimeMillis();
        stopBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("Elapsed (seconds): " + (stopMillis - startMillis) / 1000.0);
        System.out.println("Memory used at this instant (MB): " + String.format("%,.2f", (stopBytes) / 1024.0 / 1024.0));

        System.out.println("---- BoundaryAwareStream ----");
        rt.gc();
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();

        try (
                final FloatEventStream stream = service.openFloatStream(params);
                final BoundaryAwareStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false);
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
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();

        try (
                final FloatEventStream stream = service.openFloatStream(params);
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
        startBytes = rt.totalMemory() - rt.freeMemory();
        startMillis = System.currentTimeMillis();

        try (
                final FloatEventStream stream = service.openFloatStream(params);
                final FloatGraphicalEventBinSampleStream samplerStream = new FloatGraphicalEventBinSampleStream(stream, new GraphicalEventBinSamplerParams(3, count));
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
}
