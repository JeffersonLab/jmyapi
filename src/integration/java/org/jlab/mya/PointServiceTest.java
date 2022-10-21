package org.jlab.mya;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.*;


/**
 * Test the point service.
 *
 * @author slominskir
 */
public class PointServiceTest {

    private DataNexus nexus;

    @Before
    public void setUp() {
        nexus = new OnDemandNexus("docker");
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
        String pv = "channel1";
        Instant timestamp = TimeUtil.toLocalDT("2019-08-12T00:00:05");

        System.out.println("begin in myatime format: " + TimeUtil.toMyaTimestamp(timestamp));

        long start = System.currentTimeMillis();

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        long stop = System.currentTimeMillis();

        System.out.println("Metadata lookup Took: " + (stop - start) / 1000.0 + " seconds");

        System.out.println(metadata);

        float expResult = 95.21179962158203f;

        start = System.currentTimeMillis();

        FloatEvent result = nexus.findEvent(metadata, timestamp);

        stop = System.currentTimeMillis();

        System.out.println("Point lookup Took: " + (stop - start) / 1000.0 + " seconds");

        System.out.println(result);

        Assert.assertEquals(expResult, result.getValue(), 0.01);
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
        Assert.assertEquals(expResult.getValue(), result.getValue());
        Assert.assertEquals(expResult.getCode(), result.getCode());
    }

    /**
     * Test of findMultiStringEvent method, of class PointService.
     */
    @Test
    public void testFindMultiStringEvent() throws Exception {
        System.out.println("findMultiStringEvent");
        String pv = "channel3";
        Instant lessThanOrEqual = Instant.from(ZonedDateTime.of(2019, 8, 12, 1, 0, 0, 0, ZoneId.of("America/New_York")));
        Instant actual = Instant.from(ZonedDateTime.of(2019, 8, 12, 0, 0, 1, 418386347, ZoneId.of("America/New_York")));


        String value = "1.56558E9," +
                "1.56558E9," +
                "1.56558E9," +
                "1.56557E9," +
                "1.56557E9," +
                "1.56556E9," +
                "1.56556E9," +
                "1.56556E9," +
                "1.56555E9," +
                "1.56555E9," +
                "1.56555E9," +
                "1.56554E9," +
                "1.56554E9," +
                "1.56554E9," +
                "1.56553E9," +
                "1.56553E9," +
                "1.56552E9," +
                "1.56552E9," +
                "1.56552E9," +
                "1.56551E9," +
                "1.56551E9," +
                "1.56551E9," +
                "1.5655E9," +
                "1.5655E9," +
                "1.5655E9," +
                "1.56549E9," +
                "1.56549E9," +
                "1.56549E9," +
                "1.56548E9," +
                "1.56548E9," +
                "1.56547E9," +
                "1.56547E9," +
                "1.56547E9," +
                "1.56546E9," +
                "1.56546E9," +
                "1.56546E9," +
                "1.56545E9," +
                "1.56545E9," +
                "1.56545E9," +
                "1.56544E9," +
                "1.56544E9," +
                "1.56543E9," +
                "1.56543E9," +
                "1.56543E9," +
                "1.56542E9," +
                "1.56542E9," +
                "1.56542E9," +
                "1.56541E9," +
                "1.56541E9," +
                "1.56541E9," +
                "1.5654E9," +
                "1.5654E9," +
                "1.5654E9," +
                "1.56539E9," +
                "1.56539E9," +
                "1.56538E9," +
                "1.56538E9," +
                "1.56538E9," +
                "1.56537E9," +
                "1.56537E9," +
                "1.56537E9," +
                "1.56536E9," +
                "1.56536E9," +
                "1.56536E9," +
                "1.56535E9," +
                "1.56535E9," +
                "1.56534E9," +
                "1.56534E9," +
                "1.56534E9," +
                "1.56533E9," +
                "1.56533E9," +
                "1.56533E9," +
                "1.56532E9," +
                "1.56532E9," +
                "1.56532E9," +
                "1.56531E9," +
                "1.56531E9," +
                "1.56531E9," +
                "1.5653E9," +
                "1.5653E9," +
                "1.56529E9," +
                "1.56529E9," +
                "1.56529E9," +
                "1.56528E9," +
                "1.56528E9," +
                "1.56528E9," +
                "1.56527E9," +
                "1.56527E9," +
                "1.56527E9," +
                "1.56526E9," +
                "1.56526E9," +
                "1.56525E9," +
                "1.56525E9," +
                "1.56525E9," +
                "1.56524E9," +
                "1.56524E9," +
                "1.56524E9," +
                "1.56523E9," +
                "1.56523E9," +
                "1.56523E9," +
                "1.56522E9," +
                "1.56522E9," +
                "1.56522E9," +
                "1.56521E9," +
                "1.56521E9," +
                "1.5652E9," +
                "1.5652E9," +
                "1.5652E9," +
                "1.56519E9," +
                "1.56519E9," +
                "1.56519E9," +
                "1.56518E9," +
                "1.56518E9," +
                "1.56518E9," +
                "1.56517E9," +
                "1.56517E9," +
                "1.56516E9," +
                "1.56516E9," +
                "1.56516E9," +
                "1.56515E9," +
                "1.56515E9," +
                "1.56515E9," +
                "1.56514E9," +
                "1.56514E9," +
                "1.56514E9," +
                "1.56513E9," +
                "1.56513E9," +
                "1.56513E9," +
                "1.56512E9," +
                "1.56512E9," +
                "1.56511E9," +
                "1.56511E9," +
                "1.56511E9," +
                "1.5651E9," +
                "1.5651E9," +
                "1.5651E9," +
                "1.56509E9," +
                "1.56509E9," +
                "1.56509E9," +
                "1.56508E9," +
                "1.56508E9," +
                "1.56507E9," +
                "1.56507E9," +
                "1.56507E9," +
                "1.56506E9," +
                "1.56506E9," +
                "1.56506E9," +
                "1.56505E9," +
                "1.56505E9," +
                "1.56505E9," +
                "1.56504E9," +
                "1.56504E9," +
                "1.56504E9," +
                "1.56503E9," +
                "1.56503E9," +
                "1.56502E9," +
                "1.56502E9," +
                "1.56502E9," +
                "1.56501E9," +
                "1.56501E9," +
                "1.56501E9," +
                "1.565E9," +
                "1.565E9," +
                "1.565E9," +
                "1.56499E9," +
                "1.56499E9," +
                "1.56498E9," +
                "1.56498E9";
        
        MultiStringEvent expResult = new MultiStringEvent(actual,
                EventCode.UPDATE, value.split(","));

        Metadata<MultiStringEvent> metadata = nexus.findMetadata(pv, MultiStringEvent.class);

        MultiStringEvent result = nexus.findEvent(metadata, lessThanOrEqual);
        System.out.println(result.getTimestampAsInstant().atZone(ZoneId.of("America/New_York")));
        for(String v : result.getValue()) {
            System.out.println(v);
        }

        Assert.assertEquals(expResult.getTimestampAsInstant().getEpochSecond(), result.getTimestampAsInstant().getEpochSecond());
        Assert.assertArrayEquals(expResult.getValue(), result.getValue());
    }

}
