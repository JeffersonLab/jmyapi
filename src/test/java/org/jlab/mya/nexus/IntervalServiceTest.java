package org.jlab.mya.nexus;

import org.jlab.mya.EventStream;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.MultiStringEvent;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author slominskir
 */
public class IntervalServiceTest {

    private static final String HISTORY_DEPLOYMENT = "history";
    private static final String TEST_PV = "MQA3S06M";
    private static final String TEST_PV_MULTI = "HLA:bta_uxtime_h";
    private static final Instant TEST_BEGIN = TimeUtil.toLocalDT("2016-08-22T08:43:00");
    private static final Instant TEST_END = TimeUtil.toLocalDT("2017-09-22T08:43:00");

    private Metadata<FloatEvent> TEST_METADATA;
    private Metadata<MultiStringEvent> TEST_METADATA_MULTI;
    private IntervalQueryParams<FloatEvent> TEST_PARAMS;
    private IntervalQueryParams<MultiStringEvent> TEST_PARAMS_MULTI;

    private DataNexus nexus;

    @Before
    public void setUp() throws SQLException {
        nexus = new OnDemandNexus(HISTORY_DEPLOYMENT);
        TEST_METADATA = nexus.findMetadata(TEST_PV, FloatEvent.class);
        TEST_METADATA_MULTI = nexus.findMetadata(TEST_PV_MULTI, MultiStringEvent.class);
    }

    /**
     * Test of find metadata.
     */
    @Test
    public void testFindMetadata() throws Exception {
        Metadata<FloatEvent> expResult = TEST_METADATA;
        Metadata<FloatEvent> result = nexus.findMetadata(TEST_PV, FloatEvent.class);
        assertEquals(expResult, result);
    }

    /**
     * Test of count method.
     *
     * myget -c MQA3S06M -m history -b '2016-08-22 08:43:00' -e '2017-09-22 08:43:00' | wc -l
     */
    @Test
    public void testCount() throws Exception {
        long expResult = 12615L;
        long result = nexus.count(TEST_METADATA, TEST_BEGIN, TEST_END);
        assertEquals(expResult, result);
    }

    /**
     * Test of stream approach.
     */
    @Test
    public void testOpenStream() throws Exception {
        long expSize = 12615L;
        List<FloatEvent> eventList = new ArrayList<>();
        try (EventStream<FloatEvent> stream = nexus.openEventStream(TEST_METADATA, TEST_BEGIN, TEST_END)) {
            FloatEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
            }
        }
        assertEquals(expSize, eventList.size());
    }

    @Test
    public void testMultiStringEvent() throws Exception {
        long expSize = 9593;
        List<MultiStringEvent> eventList = new ArrayList<>();
        long count = nexus.count(TEST_METADATA_MULTI, TEST_BEGIN, TEST_END);
        System.out.println("count: " + count);
        try (EventStream<MultiStringEvent> stream = nexus.openEventStream(TEST_METADATA_MULTI, TEST_BEGIN, TEST_END)) {
            MultiStringEvent event;
            while ((event = stream.read()) != null) {
                eventList.add(event);
            }
        }
        assertEquals(expSize, eventList.size());
    }
}
