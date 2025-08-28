package org.jlab.mya.nexus;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.jlab.mya.stream.EventStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author slominskir
 */
public class IntervalServiceTest {

  private static final String HISTORY_DEPLOYMENT = "docker";
  private static final String TEST_PV = "channel1";
  private static final String TEST_PV_MULTI = "channel3";
  private static final Instant TEST_BEGIN = TimeUtil.toLocalDT("2019-08-12T00:00:00");
  private static final Instant TEST_END = TimeUtil.toLocalDT("2019-08-13T00:00:00");

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

  /** Test of find metadata. */
  @Test
  public void testFindMetadata() throws Exception {
    Metadata<FloatEvent> expResult = TEST_METADATA;
    Metadata<FloatEvent> result = nexus.findMetadata(TEST_PV, FloatEvent.class);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of count method.
   *
   * <p>myget -c MQA3S06M -m history -b '2016-08-22 08:43:00' -e '2017-09-22 08:43:00' | wc -l
   */
  @Test
  public void testCount() throws Exception {
    long expResult = 32990L;
    long result = nexus.count(TEST_METADATA, TEST_BEGIN, TEST_END);
    Assert.assertEquals(expResult, result);
  }

  /** Test of stream approach. */
  @Test
  public void testOpenStream() throws Exception {
    long expSize = 32990L;
    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        nexus.openEventStream(TEST_METADATA, TEST_BEGIN, TEST_END)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        eventList.add(event);
      }
    }
    Assert.assertEquals(expSize, eventList.size());
  }

  @Test
  public void testMultiStringEvent() throws Exception {
    long expSize = 24;
    List<MultiStringEvent> eventList = new ArrayList<>();
    long count = nexus.count(TEST_METADATA_MULTI, TEST_BEGIN, TEST_END);
    System.out.println("count: " + count);
    try (EventStream<MultiStringEvent> stream =
        nexus.openEventStream(TEST_METADATA_MULTI, TEST_BEGIN, TEST_END)) {
      MultiStringEvent event;
      while ((event = stream.read()) != null) {
        eventList.add(event);
      }
    }
    Assert.assertEquals(expSize, eventList.size());
  }

  /**
   * Test of large numbers with tiny changes.
   *
   * <p>myget -c iocin1:heartbeat -b '2020-05-13 09:28:00' -e '2020-05-13 09:29:00'
   */
  // @Test
  public void testLargeNumbersWithTinyChangesStream() throws Exception {

    DataNexus us = new OnDemandNexus("ops");
    Metadata<FloatEvent> metadata = us.findMetadata("iocin1:heartbeat", FloatEvent.class);

    Instant begin = TimeUtil.toLocalDT("2020-05-13T09:28:00");
    Instant end = TimeUtil.toLocalDT("2020-05-13T09:29:00");

    long expSize = 60L;
    float expLastValue = 6479405.0f;
    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = us.openEventStream(metadata, begin, end)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        eventList.add(event);
        System.out.println("(" + event.getTimestamp() + ") " + event);
      }
    }
    Assert.assertEquals(expSize, eventList.size());
    Assert.assertEquals(expLastValue, eventList.get(eventList.size() - 1).getValue(), 0.0000001);
  }

  /**
   * Test of tiny numbers with tiny changes.
   *
   * <p>myget -c VIP2L251 -b '2020-08-12 00:00:00' -e '2020-08-12 14:00:00'
   */
  // @Test
  public void testTinyNumbersWithTinyChangesStream() throws Exception {

    DataNexus us = new OnDemandNexus("ops");
    Metadata<FloatEvent> metadata = us.findMetadata("VIP2L251", FloatEvent.class);

    Instant begin = TimeUtil.toLocalDT("2020-08-12T00:00:00");
    Instant end = TimeUtil.toLocalDT("2020-08-12T14:00:00");

    long expSize = 58L;
    float expLastValue = 1e-11f;
    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = us.openEventStream(metadata, begin, end)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        eventList.add(event);
        System.out.println("(" + event.getTimestamp() + ") " + event);
      }
    }
    Assert.assertEquals(expSize, eventList.size());
    Assert.assertEquals(expLastValue, eventList.get(eventList.size() - 1).getValue(), 0.0000001);
  }
}
