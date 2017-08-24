package org.jlab.mya.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
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
 *
 * @author ryans
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
        DataNexus nexus = new OnDemandNexus(Deployment.ops);
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
        PointQueryParams params = null;
        PointService instance = null;
        IntEvent expResult = null;
        IntEvent result = instance.findIntEvent(params);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of findMultiStringEvent method, of class PointService.
     */
    @Test
    public void testFindMultiStringEvent() throws Exception {
        System.out.println("findMultiStringEvent");
        PointQueryParams params = null;
        PointService instance = null;
        MultiStringEvent expResult = null;
        MultiStringEvent result = instance.findMultiStringEvent(params);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
