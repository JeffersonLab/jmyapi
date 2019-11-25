package org.jlab.mya.nexus;

import java.time.Instant;

import org.jlab.mya.TimeUtil;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.Metadata;
import org.jlab.mya.StandaloneConnectionPools;
import org.jlab.mya.StandaloneJndi;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.service.PointService;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test the pooled nexus.
 * 
 * @author slominskir
 */
public class PooledNexusTest {

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
            Instant timestamp = TimeUtil.toLocalDT("2017-01-01T00:00:05");

            Metadata<FloatEvent> metadata = service.findMetadata(pv, FloatEvent.class);
            PointQueryParams<FloatEvent> params = new PointQueryParams<>(metadata, timestamp);
            float expResult = -7.2f;
            FloatEvent result = service.findFloatEvent(params);
            assertEquals(expResult, result.getValue(), 0.01);
            System.out.println(result);
        }
    }

}
