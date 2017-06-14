package org.jlab.mya;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.FloatEventStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author ryans
 */
public class UnitTests {

    public static final Deployment TEST_DEPLOYMENT = Deployment.dev;
    public static final String TEST_PV = "DCPHP2ADC10";
    public static final int TEST_ID = 13;
    public static final Class TEST_CLASS = Float.class;
    public static final int TEST_SIZE = 1;
    public static final Metadata TEST_METADATA = new Metadata(TEST_ID, TEST_PV, "devmya0",
            DataType.DBR_DOUBLE, 1);
    public static final Instant TEST_BEGIN = LocalDateTime.parse("2016-08-22T08:43:00").atZone(ZoneId.systemDefault()).toInstant();
    public static final Instant TEST_END = LocalDateTime.parse("2017-09-22T08:43:00").atZone(ZoneId.systemDefault()).toInstant();
    public static final QueryParams TEST_PARAMS = new QueryParams(TEST_METADATA, TEST_BEGIN,
            TEST_END);

    private QueryService service;

    public UnitTests() {

    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        DataNexus nexus = new OnDemandNexus(TEST_DEPLOYMENT);
        service = new QueryService(nexus);
    }

    @After
    public void tearDown() throws SQLException {

    }

    /**
     * Test of fetchMetadata method, of class MyGet.
     */
    @Test
    public void testFindMetadata() throws Exception {
        Metadata expResult = TEST_METADATA;
        Metadata result = service.findMetadata(TEST_PV);
        assertEquals(expResult, result);
    }

    /**
     * Test of countRecords method, of class MyGet.
     */
    @Test
    public void testCount() throws Exception {
        long expResult = 0L;
        long result = service.count(TEST_PARAMS);
        assertEquals(expResult, result);
    }

    /**
     * Test of fetchList method, of class MyGet.
     */
    @Test
    public void testOpenStream() throws Exception {
        long expSize = 0;
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = service.openFloat(TEST_PARAMS)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
            }
        }
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }

}
