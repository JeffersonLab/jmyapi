package org.jlab.mya.nexus;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.jlab.mya.EventCode;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Test the point service.
 *
 * @author slominskir
 */
public class PointServiceTest {

    private DataNexus nexus;

    @Before
    public void setUp() {
        nexus = new OnDemandNexus("history");
    }

    /**
     * Test of findFloatEvent method, of class PointService.
     *
     * <pre>
     * Compare with:
     * myget -c R123PMES -m history -t "2017-01-01 00:00:05"
     *
     * mysql -A -h hstmya1 -u myapi -D archive -p
     * select * from table_32108 force index for order by (primary) where time <= 398156032460718080 order by time desc limit 1;
     *</pre>
     * <p>
     * Note: without query hint: "force index for order by (primary)" this unit test takes
     * over 25 seconds vs less than 0.5 seconds
     * </p>
     */
    @Test
    public void testFindFloatEvent() throws Exception {
        System.out.println("findFloatEvent");
        String pv = "R123PMES";
        Instant timestamp = TimeUtil.toLocalDT("2017-01-01T00:00:05");

        System.out.println("begin in myatime format: " + TimeUtil.toMyaTimestamp(timestamp));

        long start = System.currentTimeMillis();

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        long stop = System.currentTimeMillis();

        System.out.println("Metadata lookup Took: " + (stop - start) / 1000.0 + " seconds");

        System.out.println(metadata);

        float expResult = -7.2f;

        start = System.currentTimeMillis();

        FloatEvent result = nexus.findEvent(metadata, timestamp);

        stop = System.currentTimeMillis();

        System.out.println("Point lookup Took: " + (stop - start) / 1000.0 + " seconds");

        System.out.println(result);

        assertEquals(expResult, result.getValue(), 0.01);
    }

    /**
     * Test of findIntEvent method, of class PointService.
     */
    @Test
    public void testFindIntEvent() throws Exception {
        System.out.println("findIntEvent");

        String pv = "MFELINJC";
        Instant timestamp = TimeUtil.toLocalDT("2017-09-08T14:50:38");
        Metadata<IntEvent> metadata = nexus.findMetadata(pv, IntEvent.class);
        
        // Roughly 2017-09-08 14:50:37
        IntEvent expResult = new IntEvent(403967615077170415L, EventCode.UPDATE, 0);
        IntEvent result =  nexus.findEvent(metadata, timestamp);

        Assert.assertEquals(expResult.getTimestamp(), result.getTimestamp());
        assertEquals(expResult.getValue(), result.getValue());
        Assert.assertEquals(expResult.getCode(), result.getCode());
    }

    /**
     * Test of findMultiStringEvent method, of class PointService.
     */
    @Test
    public void testFindMultiStringEvent() throws Exception {
        System.out.println("findMultiStringEvent");
        String pv = "HLA:bta_uxtime_h";
        Instant time = Instant.from(ZonedDateTime.of(2017, 8, 21, 0, 0, 0, 0, ZoneId.of("America/New_York")));

        String value = "1.50328e+09,1.50328e+09,1.50328e+09,1.50327e+09,1.50327e+09,1.50327e+09,1.50326e+09,1.50326e+09,"
                + "1.50326e+09,1.50325e+09,1.50325e+09,1.50324e+09,1.50324e+09,1.50324e+09,1.50323e+09,1.50323e+09,"
                + "1.50323e+09,1.50322e+09,1.50322e+09,1.50322e+09,1.50321e+09,1.50321e+09,1.50321e+09,1.5032e+09,"
                + "1.5032e+09,1.50319e+09,1.50319e+09,1.50319e+09,1.50318e+09,1.50318e+09,1.50318e+09,1.50317e+09,"
                + "1.50317e+09,1.50317e+09,1.50316e+09,1.50316e+09,1.50315e+09,1.50315e+09,1.50315e+09,1.50314e+09,"
                + "1.50314e+09,1.50314e+09,1.50313e+09,1.50313e+09,1.50313e+09,1.50312e+09,1.50312e+09,1.50312e+09,"
                + "1.50311e+09,1.50311e+09,1.5031e+09,1.5031e+09,1.5031e+09,1.50309e+09,1.50309e+09,1.50309e+09,"
                + "1.50308e+09,1.50308e+09,1.50308e+09,1.50307e+09,1.50307e+09,1.50306e+09,1.50306e+09,1.50306e+09,"
                + "1.50305e+09,1.50305e+09,1.50305e+09,1.50304e+09,1.50304e+09,1.50304e+09,1.50303e+09,1.50303e+09,"
                + "1.50303e+09,1.50302e+09,1.50302e+09,1.50301e+09,1.50301e+09,1.50301e+09,1.503e+09,1.503e+09,"
                + "1.503e+09,1.50299e+09,1.50299e+09,1.50299e+09,1.50298e+09,1.50298e+09,1.50297e+09,1.50297e+09,"
                + "1.50297e+09,1.50296e+09,1.50296e+09,1.50296e+09,1.50295e+09,1.50295e+09,1.50295e+09,1.50294e+09,"
                + "1.50294e+09,1.50294e+09,1.50293e+09,1.50293e+09,1.50292e+09,1.50292e+09,1.50292e+09,1.50291e+09,"
                + "1.50291e+09,1.50291e+09,1.5029e+09,1.5029e+09,1.5029e+09,1.50289e+09,1.50289e+09,1.50288e+09,"
                + "1.50288e+09,1.50288e+09,1.50287e+09,1.50287e+09,1.50287e+09,1.50286e+09,1.50286e+09,1.50286e+09,"
                + "1.50285e+09,1.50285e+09,1.50285e+09,1.50284e+09,1.50284e+09,1.50283e+09,1.50283e+09,1.50283e+09,"
                + "1.50282e+09,1.50282e+09,1.50282e+09,1.50281e+09,1.50281e+09,1.50281e+09,1.5028e+09,1.5028e+09,"
                + "1.50279e+09,1.50279e+09,1.50279e+09,1.50278e+09,1.50278e+09,1.50278e+09,1.50277e+09,1.50277e+09,"
                + "1.50277e+09,1.50276e+09,1.50276e+09,1.50276e+09,1.50275e+09,1.50275e+09,1.50274e+09,1.50274e+09,"
                + "1.50274e+09,1.50273e+09,1.50273e+09,1.50273e+09,1.50272e+09,1.50272e+09,1.50272e+09,1.50271e+09,"
                + "1.50271e+09,1.5027e+09,1.5027e+09,1.5027e+09,1.50269e+09,1.50269e+09,1.50269e+09,1.50268e+09";
        
        MultiStringEvent expResult = new MultiStringEvent(Instant.from(ZonedDateTime.of(2017,8, 20, 23, 0, 0, 0, ZoneId.of("America/New_York"))),
                EventCode.UPDATE, value.split(","));

        Metadata<MultiStringEvent> metadata = nexus.findMetadata(pv, MultiStringEvent.class);

        MultiStringEvent result = nexus.findEvent(metadata, time);
        System.out.println(result.getTimestampAsInstant().toString());
        for(String v : result.getValue()) {
            System.out.println(v);
        }
        assertArrayEquals(expResult.getValue(), result.getValue());
        Assert.assertEquals(expResult.getTimestampAsInstant().getEpochSecond(), result.getTimestampAsInstant().getEpochSecond());
    }

}
