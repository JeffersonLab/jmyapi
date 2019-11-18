package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.stream.ListStream;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BoundaryAwareTest {

    /**
     * Test boundary aware stream
     */
    @Test
    public void testBoundaryAwareStream() throws Exception {
        Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2019-06-01T00:00:00");

        List<FloatEvent> events = new ArrayList<FloatEvent>();
        events.add(new FloatEvent(TimeUtil.toLocalDT("2019-02-01T00:00:00"), EventCode.UPDATE, 1));
        events.add(new FloatEvent(TimeUtil.toLocalDT("2019-03-01T00:00:00"), EventCode.UPDATE, 2));
        events.add(new FloatEvent(TimeUtil.toLocalDT("2019-04-01T00:00:00"), EventCode.UPDATE, 3));
        events.add(new FloatEvent(TimeUtil.toLocalDT("2019-05-01T00:00:00"), EventCode.UPDATE, 4));

        FloatEvent priorPoint = new FloatEvent(TimeUtil.toLocalDT("2018-12-01T00:00:00"), EventCode.UPDATE, 0);

        long count = events.size();

        System.out.println("count: " + count);

        long expSize = 6;

        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = new ListStream<FloatEvent>(events, FloatEvent.class)) {
            try (BoundaryAwareStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class)) {
                FloatEvent event;
                while ((event = boundaryStream.read()) != null) {
                    eventList.add(event);
                    System.out.println(event.toString());
                }
            }
        }

        assertEquals("List size does not match expected", expSize, eventList.size());
    }
}
