package org.jlab.mya;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ryans
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
        //String dateStr = "2017-01-01T00:00:01.749013";
        String dateStr = "2017-01-01T00:00:00.749013326";

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

        //String dateStr = "2017-01-01T00:00:01.749013";
        String dateStr = "2017-01-01T00:00:00.749013326";

        long timestamp = 6370496505408607835L;
        Instant expResult = LocalDateTime.parse(dateStr).atZone(
                ZoneId.systemDefault()).toInstant();
        Instant result = TimeUtil.fromMyaTimestamp(timestamp);
        assertEquals(expResult, result);
    }

}
