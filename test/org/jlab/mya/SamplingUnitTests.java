package org.jlab.mya;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.NaiveSamplerParams;
import org.jlab.mya.service.SamplingService;
import org.jlab.mya.stream.FloatEventStream;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.fail;
import static org.junit.Assert.fail;

/**
 *
 * @author ryans
 */
public class SamplingUnitTests {
    public static final Deployment TEST_DEPLOYMENT = Deployment.ops;
    public static final String TEST_PV = "R123PMES";
    public static final Instant TEST_BEGIN = LocalDateTime.parse("2017-01-01T00:00:00").atZone(
            ZoneId.systemDefault()).toInstant();
    public static final Instant TEST_END = LocalDateTime.parse("2017-01-25T00:00:00").atZone(
            ZoneId.systemDefault()).toInstant();
    public static final long TEST_NAIVE_LIMIT = 24;

    private SamplingService service;
    private Metadata TEST_METADATA;
    private NaiveSamplerParams TEST_NAIVE_PARAMS;

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        DataNexus nexus = new OnDemandNexus(TEST_DEPLOYMENT);
        service = new SamplingService(nexus);
        TEST_METADATA = service.findMetadata(TEST_PV);
        TEST_NAIVE_PARAMS = new NaiveSamplerParams(TEST_METADATA, TEST_BEGIN,
                TEST_END, TEST_NAIVE_LIMIT);

    }

    @After
    public void tearDown() throws SQLException {

    }    
    
    /**
     * Test basic sample
     */
    @Test
    public void testNaiveSampler() throws Exception {
        long expSize = 21; // We limit to 24, but we know historical data only has 21
        List<FloatEvent> eventList = new ArrayList<>();
        try (FloatEventStream stream = service.openFloatNaiveSampler(TEST_NAIVE_PARAMS)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
                System.out.println(event);
            }
        }
        if (eventList.size() != expSize) {
            fail("List size does not match expected");
        }
    }    
}
