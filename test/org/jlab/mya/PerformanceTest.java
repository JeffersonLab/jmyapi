package org.jlab.mya;

import org.jlab.mya.service.IntervalService;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.FloatEventStream;
import org.junit.Test;

/**
 *
 * @author slominskir
 */
public class PerformanceTest {

    @Test
    public void doTest() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus(Deployment.ops);
        IntervalService service = new IntervalService(nexus);

        String pv = "IDC1G03sh_cur";
        Instant begin
                = LocalDateTime.parse("2016-06-01T00:00:00").atZone(ZoneId.systemDefault()).toInstant();
        Instant end
                = LocalDateTime.parse("2017-07-01T00:00:00").atZone(ZoneId.systemDefault()).toInstant();

        Metadata metadata = service.findMetadata(pv);

        QueryParams params = new QueryParams(metadata, begin, end);

        Runtime rt = Runtime.getRuntime();
        long memoryUsedBytes;
        long start;
        long stop;

        memoryUsedBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("memory used MB: " + (memoryUsedBytes / 1024.0 / 1024.0));

        start = System.currentTimeMillis();
        long count = service.count(params);
        stop = System.currentTimeMillis();
        System.out.println("count: " + count + "; took seconds: " + (stop - start) / 1000.0);
        start = System.currentTimeMillis();
        try (FloatEventStream stream = service.openFloat(params)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                //System.out.println(event);
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("Stream Traversed seconds: " + (stop - start) / 1000.0);

        memoryUsedBytes = rt.totalMemory() - rt.freeMemory();
        System.out.println("memory used MB: " + (memoryUsedBytes / 1024.0 / 1024.0));
    }
}
