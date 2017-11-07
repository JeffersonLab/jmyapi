package org.jlab.mya.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
import org.jlab.mya.EventCode;
import org.jlab.mya.Metadata;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.PointQueryParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the point service.
 *
 * @author slominskir
 */
public class PointServiceTest {

    private PointService service;

    public PointServiceTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        DataNexus nexus = new OnDemandNexus(Deployment.opsfb);
        service = new PointService(nexus);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of findFloatEvent method, of class PointService.
     */
    @Test
    public void testFindFloatEvent() throws Exception {
        System.out.println("findFloatEvent");
        String pv = "R123PMES";
        Instant timestamp = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
                ZoneId.systemDefault()).toInstant();

        Metadata metadata = service.findMetadata(pv);
        PointQueryParams params = new PointQueryParams(metadata, timestamp);
        float expResult = -7.2f;
        FloatEvent result = service.findFloatEvent(params);
        assertEquals(expResult, result.getValue(), 0.01);
    }

    /**
     * Test of findIntEvent method, of class PointService.
     */
    @Test
    public void testFindIntEvent() throws Exception {
        System.out.println("findIntEvent");

        String pv = "MFELINJC";
        Instant timestamp = LocalDateTime.parse("2017-10-19T00:00:00").atZone(ZoneId.systemDefault()).toInstant();
        Metadata metadata = service.findMetadata(pv);        
        PointQueryParams params = new PointQueryParams(metadata, timestamp);
        
        // Roughly 2017-10-18 14:43:15
        IntEvent expResult = new IntEvent(6478323349793338213L, EventCode.UPDATE, 0);
        IntEvent result =  service.findIntEvent(params);

        assertEquals(expResult.getTimestamp(), result.getTimestamp());
        assertEquals(expResult.getValue(), result.getValue());
        assertEquals(expResult.getCode(), result.getCode());
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

        Metadata metadata = service.findMetadata(pv);
        PointQueryParams params = new PointQueryParams(metadata, time);

        MultiStringEvent result = service.findMultiStringEvent(params);
        System.out.println(result.getTimestampAsInstant().toString());
        for(String v : result.getValue()) {
            System.out.println(v);
        }
        assertArrayEquals(expResult.getValue(), result.getValue());
        assertEquals(expResult.getTimestampAsInstant().getEpochSecond(), result.getTimestampAsInstant().getEpochSecond());
    }

}
