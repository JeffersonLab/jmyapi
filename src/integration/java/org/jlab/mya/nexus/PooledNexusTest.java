package org.jlab.mya.nexus;

import java.time.Instant;

import org.jlab.mya.TimeUtil;
import org.jlab.mya.Metadata;
import org.jlab.mya.StandaloneJndi;
import org.jlab.mya.event.FloatEvent;
import org.junit.Assert;
import org.junit.Test;

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
        try (StandaloneConnectionPools pools = new StandaloneConnectionPools("docker")) {
            System.out.println("After try");
            PooledNexus nexus = new PooledNexus("docker");
            PointService service = new PointService(nexus);
            String pv = "channel1";
            Instant timestamp = TimeUtil.toLocalDT("2019-08-12T01:00:00");

            Metadata<FloatEvent> metadata = service.findMetadata(pv, FloatEvent.class);
            PointQueryParams<FloatEvent> params = new PointQueryParams<>(metadata, timestamp);
            float expResult = 95.30329895019531f;
            FloatEvent result = service.findFloatEvent(params);
            Assert.assertEquals(expResult, result.getValue(), 0.01);
            System.out.println(result);
        }
    }

}
