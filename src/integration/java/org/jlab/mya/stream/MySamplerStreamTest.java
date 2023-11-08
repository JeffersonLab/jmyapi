package org.jlab.mya.stream;

import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MySamplerStreamTest {
    /**
     * Run a test on a very busy channel.  A single query is performance limited by how long it takes to the time it
     * takes to stream the entire channel unless we do something clever.  Let's make sure we don't end up taking too
     * long.
     * @throws IOException
     */
    @Test
    public void busyChannelTest() throws IOException, SQLException {
        DataNexus nexus = new OnDemandNexus("docker");

        String pv = "channel5";
        Instant begin = TimeUtil.toLocalDT("2022-12-01T13:40:46");
        Instant end = TimeUtil.toLocalDT("2023-01-17T03:00:00");
        long stepMilliseconds = 840_000_000L;
        long sampleCount = 8;
        boolean updatesOnly = false;


        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        FloatEvent priorPoint = nexus.findEvent(metadata, begin, true, true, false);

        long count = 0;
        long startMillis = Instant.now().toEpochMilli();
        System.out.println("Starting: " + startMillis);
        try (EventStream<FloatEvent> stream = MySamplerStream.getMySamplerStream(nexus.openEventStream(metadata, begin, end),
                begin, stepMilliseconds, sampleCount, priorPoint, updatesOnly, FloatEvent.class, nexus, metadata)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                System.out.println(event);
                count++;
            }
        }
        long stopMillis = Instant.now().toEpochMilli();
        System.out.println("Done: " + stopMillis);

        System.out.println("Took " + count + " samples in " + (stopMillis - startMillis) + " ms");

        count = 0;
        startMillis = Instant.now().toEpochMilli();
        System.out.println("Starting: " + startMillis);
        FloatEvent last = null;
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                if (count < 5) {
                    System.out.println(event.getTimestampAsInstant().getNano());
                }
                count++;
                last = event;
            }
            System.out.println(last);
        }
        stopMillis = Instant.now().toEpochMilli();
        System.out.println("Done: " + stopMillis);

        System.out.println("Streamed " + count + " events in " + (stopMillis - startMillis) + " ms");
    }


}
