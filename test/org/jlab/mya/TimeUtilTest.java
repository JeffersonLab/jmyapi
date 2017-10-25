package org.jlab.mya;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.service.IntervalService;
import org.jlab.mya.stream.FloatEventStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the TimeUtil.
 * 
 * @author slominskir
 */
public class TimeUtilTest {

    public TimeUtilTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of toMyaTimestamp method, of class TimeUtil.
     */
    @Test
    public void testToMyaTimestamp() {
        System.out.println("toMyaTimestamp");
        String dateStr = "2017-01-01T00:00:01.749013325";

        Instant instant = LocalDateTime.parse(dateStr).atZone(
                ZoneId.systemDefault()).toInstant();
        long expResult = 6370496505408607835L;
        long result = TimeUtil.toMyaTimestamp(instant);
        assertEquals(expResult, result);
    }

    /**
     * Test of fromMyaTimestamp method, of class TimeUtil.
     */
    @Test
    public void testFromMyaTimestamp() {
        System.out.println("fromMyaTimestamp");

        String dateStr = "2017-01-01T00:00:01.749013325";

        long timestamp = 6370496505408607835L;
        Instant expResult = LocalDateTime.parse(dateStr).atZone(
                ZoneId.systemDefault()).toInstant();
        Instant result = TimeUtil.fromMyaTimestamp(timestamp);
        assertEquals(expResult, result);
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
        DataNexus nexus = new OnDemandNexus(Deployment.opsfb);
        IntervalService service = new IntervalService(nexus);
        Metadata metadata = service.findMetadata("R123PMES");
        Instant begin = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-02-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();
        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);
        try (FloatEventStream stream = service.openFloatStream(params)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                long t = TimeUtil.toMyaTimestamp(event.getTimestampAsInstant());
                Instant i = TimeUtil.fromMyaTimestamp(t);
                assertEquals(event.getTimestampAsInstant().getNano(), i.getNano(), delta);
            }
        }
    }

}
