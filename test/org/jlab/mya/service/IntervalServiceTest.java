package org.jlab.mya.service;

import org.jlab.mya.params.IntervalQueryParams;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
import org.jlab.mya.Metadata;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.MultiStringEventStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author slominskir
 */
public class IntervalServiceTest {

    public static final Deployment DEV_DEPLOYMENT = Deployment.dev;
    public static final Deployment OPSFB_DEPLOYMENT = Deployment.opsfb;
    public static final String TEST_PV = "DCPHP2ADC10";
    public static final String TEST_PV_MULTI = "HLA:bta_uxtime_h";
    public static final Instant TEST_BEGIN = LocalDateTime.parse("2016-08-22T08:43:00").atZone(
            ZoneId.systemDefault()).toInstant();
    public static final Instant TEST_END = LocalDateTime.parse("2017-09-22T08:43:00").atZone(
            ZoneId.systemDefault()).toInstant();

    private IntervalService serviceDev;
    private IntervalService serviceOpsFB;
    private Metadata TEST_METADATA;
    private Metadata TEST_METADATA_MULTI;
    private IntervalQueryParams TEST_PARAMS;
    private IntervalQueryParams TEST_PARAMS_MULTI;

    public IntervalServiceTest() {

    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        DataNexus nexusDev = new OnDemandNexus(DEV_DEPLOYMENT);
        DataNexus nexusOpsFB = new OnDemandNexus(OPSFB_DEPLOYMENT);
        serviceDev = new IntervalService(nexusDev);
        serviceOpsFB = new IntervalService(nexusOpsFB);
        TEST_METADATA = serviceDev.findMetadata(TEST_PV);
        TEST_METADATA_MULTI = serviceOpsFB.findMetadata(TEST_PV_MULTI);
        TEST_PARAMS = new IntervalQueryParams(TEST_METADATA, TEST_BEGIN, TEST_END);
        TEST_PARAMS_MULTI = new IntervalQueryParams(TEST_METADATA_MULTI, TEST_BEGIN, TEST_END);
    }

    @After
    public void tearDown() throws SQLException {

    }

    /**
     * Test of find metadata.
     */
    @Test
    public void testFindMetadata() throws Exception {
        Metadata expResult = TEST_METADATA;
        Metadata result = serviceDev.findMetadata(TEST_PV);
        assertEquals(expResult, result);
    }

    /**
     * Test of countRecords method, of class MyGet.
     */
    @Test
    public void testCount() throws Exception {
        long expResult = 6104308L;
        long result = serviceDev.count(TEST_PARAMS);
        assertEquals(expResult, result);
    }

    /**
     * Test of stream approach.
     */
    @Test
    public void testOpenStream() throws Exception {
        long expSize = 6104308L;
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = serviceDev.openFloatStream(TEST_PARAMS)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
            }
        }
        assertEquals(expSize, eventList.size());
    }

    @Test
    public void testMultiStringEvent() throws Exception {
        long expSize = 9553;
        List<MultiStringEvent> eventList = new ArrayList<>();
        long count = serviceOpsFB.count(TEST_PARAMS_MULTI);
        System.out.println("count: " + count);
        try (MultiStringEventStream stream = serviceOpsFB.openMultiStringStream(TEST_PARAMS_MULTI)) {
            MultiStringEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
            }
        }
        assertEquals(expSize, eventList.size());
    }
}
