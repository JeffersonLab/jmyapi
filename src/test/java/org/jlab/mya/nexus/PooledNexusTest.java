package org.jlab.mya.nexus;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import main.org.org.jlab.mya.params.PointQueryParams;
import main.org.org.jlab.mya.Metadata;
import org.jlab.mya.StandaloneConnectionPools;
import org.jlab.mya.StandaloneJndi;
import main.org.org.jlab.mya.event.FloatEvent;
import main.org.org.jlab.mya.service.PointService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the pooled nexus.
 * 
 * @author slominskir
 */
public class PooledNexusTest {

    public PooledNexusTest() {
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
     * Test of getConnection method, of class PooledNexus.
     */
    @Test
    public void testGetConnection() throws Exception {
        System.out.println("getConnection");
        new StandaloneJndi();
        try (StandaloneConnectionPools pools = new StandaloneConnectionPools("history")) {
            System.out.println("After try");
            PooledNexus nexus = new PooledNexus("history");
            PointService service = new PointService(nexus);
            String pv = "R123PMES";
            Instant timestamp = LocalDateTime.parse("2017-01-01T00:00:05").atZone(
                    ZoneId.systemDefault()).toInstant();

            Metadata metadata = service.findMetadata(pv);
            PointQueryParams params = new PointQueryParams(metadata, timestamp);
            float expResult = -7.2f;
            FloatEvent result = service.findFloatEvent(params);
            assertEquals(expResult, result.getValue(), 0.01);
            System.out.println(result);
        }
    }

}
