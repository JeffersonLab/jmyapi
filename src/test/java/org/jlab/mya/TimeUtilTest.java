package org.jlab.mya;

import java.time.Instant;

import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.IntervalQueryParams;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Test the TimeUtil.
 * 
 * @author slominskir
 */
public class TimeUtilTest {

    /**
     * Test of toMyaTimestamp method, of class TimeUtil.
     */
    @Test
    public void testToMyaTimestamp() {
        System.out.println("toMyaTimestamp");
        String dateStr = "2017-01-01T00:00:01.749013325";

        Instant instant = TimeUtil.toLocalDT(dateStr);
        long expResult = 398156031588037989L;
        long result = TimeUtil.toMyaTimestamp(instant);
        assertEquals(expResult, result, 4);
        
    }

    /**
     * Test of fromMyaTimestamp method, of class TimeUtil.
     */
    @Test
    public void testFromMyaTimestamp() {
        System.out.println("fromMyaTimestamp");
        String dateStr = "2017-01-01T00:00:01.749013325";

        long timestamp = 398156031588037989L;
        Instant expResult = TimeUtil.toLocalDT(dateStr);
        Instant result = TimeUtil.fromMyaTimestamp(timestamp);
        assertEquals(expResult.getNano(), result.getNano(), 1);
    }

    /**
     * Make sure nanosecond difference in conversion functions is no more than 100 and do this test
     * over an interval of many events.
     *
     * @throws Exception if something bad happened
     */
    @Test
    public void testABunchOData() throws Exception {
        float delta = 100; // nanoseconds of fudge
        DataNexus nexus = new OnDemandNexus("history");
        Metadata<FloatEvent> metadata = nexus.findMetadata("R123PMES", FloatEvent.class);
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00");
        Instant end = TimeUtil.toLocalDT("2017-02-01T00:00:00");
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                long t = TimeUtil.toMyaTimestamp(event.getTimestampAsInstant());
                Instant i = TimeUtil.fromMyaTimestamp(t);
                Assert.assertEquals(event.getTimestampAsInstant().getNano(), i.getNano(), delta);
            }
        }
    }
}
