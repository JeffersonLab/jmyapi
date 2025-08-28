package org.jlab.mya.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.Event;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.Test;

@SuppressWarnings("DuplicatedCode")
public class MySamplerStreamTest {

  public <T extends Event> List<T> getStreamedSamples(
      Instant begin,
      Instant end,
      long intervalMillis,
      long sampleCount,
      boolean updatesOnly,
      Class<T> type,
      DataNexus nexus,
      Metadata<T> metadata)
      throws SQLException, IOException {

    List<T> samples = new ArrayList<>();
    T priorPoint = nexus.findEvent(metadata, begin, true, true, updatesOnly);
    try (EventStream<T> stream =
        MySamplerStream.getMySamplerStream(
            nexus.openEventStream(metadata, begin, end),
            begin,
            intervalMillis,
            sampleCount,
            priorPoint,
            updatesOnly,
            type)) {
      T event;
      while ((event = stream.read()) != null) {
        samples.add(event);
      }
    }
    return samples;
  }

  public <T extends Event> List<T> getQueriedSamples(
      Instant begin,
      long intervalMillis,
      long sampleCount,
      boolean updatesOnly,
      Class<T> type,
      DataNexus nexus,
      Metadata<T> metadata)
      throws IOException {
    List<T> samples = new ArrayList<>();
    try (EventStream<T> stream =
        MySamplerStream.getMySamplerStream(
            begin, intervalMillis, sampleCount, updatesOnly, type, nexus, metadata)) {
      T event;
      while ((event = stream.read()) != null) {
        samples.add(event);
      }
    }
    return samples;
  }

  public static void timeStream(
      String pv,
      Instant begin,
      Instant end,
      long stepMilliseconds,
      long sampleCount,
      boolean updatesOnly,
      DataNexus nexus)
      throws SQLException, IOException {

    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    FloatEvent priorPoint = nexus.findEvent(metadata, begin, true, true, false);

    long count = 0;
    long startMillis = Instant.now().toEpochMilli();
    MySamplerStream.Strategy strategy;
    try (MySamplerStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            nexus.openEventStream(metadata, begin, end),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            FloatEvent.class)) {
      while (stream.read() != null) {
        count++;
      }
      strategy = stream.getStrategy();
    }
    long stopMillis = Instant.now().toEpochMilli();
    double events_per_sample = stepMilliseconds / 100.0;
    System.out.println(
        events_per_sample
            + ","
            + strategy.toString()
            + ": "
            + count
            + " samples in "
            + (stopMillis - startMillis)
            + " ms");
  }

  public static void timeNQueries(
      String pv,
      Instant begin,
      long stepMilliseconds,
      long sampleCount,
      boolean updatesOnly,
      DataNexus nexus)
      throws SQLException, IOException {

    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

    long count = 0;
    long startMillis = Instant.now().toEpochMilli();
    MySamplerStream.Strategy strategy;
    try (MySamplerStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            begin, stepMilliseconds, sampleCount, updatesOnly, FloatEvent.class, nexus, metadata)) {
      while (stream.read() != null) {
        count++;
      }
      strategy = stream.getStrategy();
    }
    long stopMillis = Instant.now().toEpochMilli();
    double events_per_sample = stepMilliseconds / 100.0;
    System.out.println(
        events_per_sample
            + ","
            + strategy.toString()
            + ": "
            + count
            + " samples in "
            + (stopMillis - startMillis)
            + " ms");
  }

  /**
   * Run a test on a very busy channel. A single query is performance limited by how long it takes
   * to the time it takes to stream the entire channel unless we do something clever. Since we've
   * currently abandoned a built-in hybrid approach, this is of a more of an academic interest. This
   * test is commented out as it is more of a study in algorithmic tuning.
   *
   * @throws IOException If trouble querying data
   */
  //    @Test
  @SuppressWarnings("unused")
  public void busyChannelTuningTest() throws IOException, SQLException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    // First event at 2022-12-04 10:50:56
    Instant begin = TimeUtil.toLocalDT("2022-12-04T10:50:55");
    // Last event at 2022-12-05 16:48:47
    Instant end = TimeUtil.toLocalDT("2022-12-05T17:00:00");
    long sampleCount = 10;
    boolean updatesOnly = false;
    int bufferSize = 50;
    int bufferCheck = 10_000;
    int bufferDelay = 50;

    long[] steps =
        new long[] {
          840_000_000L,
          340_000_000L,
          170_000_000L,
          84_000_000L,
          42_000_000L,
          21_000_000L,
          10_500_000L,
          3_600_000L,
          2_700_000L,
          1_800_000L,
          900_000L,
          600_000L,
          450_000L,
          300_000L,
          230_000L,
          60_000L
        };

    System.out.println("Events/Sample,Threshold,Strategy: Results");
    for (long s : steps) {
      timeStream(pv, begin, end, s, sampleCount, updatesOnly, nexus);
      timeNQueries(pv, begin, s, sampleCount, updatesOnly, nexus);
    }
    System.out.println();

    // Print out the first few and last events.
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    long count = 0;
    long startMillis = Instant.now().toEpochMilli();
    System.out.println();
    System.out.println();
    System.out.println("Starting: " + startMillis);
    FloatEvent last = null;
    try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        if (count < 3) {
          System.out.println(event);
        }
        count++;
        last = event;
      }
      System.out.println(last);
    }
    long stopMillis = Instant.now().toEpochMilli();
    System.out.println("Done: " + stopMillis);

    System.out.println("Streamed " + count + " events in " + (stopMillis - startMillis) + " ms");
  }

  /**
   * Test that all three strategies return the same result when the first point will be undefined
   * (prior to stream). Check non-default buffer/threshold params.
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalenceParams1() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-04T10:49:59");
    Instant end = TimeUtil.toLocalDT("2023-01-17T03:00:00");
    long sampleCount = 10;
    long intervalMillis = 1_000;
    boolean updatesOnly = false;

    testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);
  }

  /**
   * Test that all three strategies return the same result when the first point will be undefined
   * (prior to stream). Check non-default buffer/threshold params, and updatesOnly == true.
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalenceParams2() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-05T14:49:59");
    Instant end = TimeUtil.toLocalDT("2023-01-17T03:00:00");
    long sampleCount = 10;
    long intervalMillis = 1_000_000_000; //
    boolean updatesOnly = true;

    testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);
  }

  /**
   * Test that all three strategies return the same result when the first point will be undefined
   * (prior to stream). Check default buffer/threshold params.
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalenceParams3() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-04T10:49:59");
    Instant end = TimeUtil.toLocalDT("2023-01-17T03:00:00");
    long sampleCount = 20;
    long intervalMillis = 8_640_000; // 1 day. Everything except the 2nd sample should be UNDEFINED.
    boolean updatesOnly = false;

    List<FloatEvent> stream =
        testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);

    FloatEvent e = stream.get(0);
    // Make sure the first value is what we expect.  It should be UNDEFINED at the same time as
    // begin.
    assertEquals(e.toString(), EventCode.UNDEFINED, e.getCode());
    assertEquals(e.toString(), 0, e.getValue(), 1e-10);
    assertEquals(
        e.toString(), TimeUtil.toLocalDT("2022-12-04T10:49:59"), e.getTimestampAsInstant());
  }

  /**
   * Test that all three strategies return the same result when the first point will be undefined
   * (prior to stream).
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalence1() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-04T10:49:59");
    Instant end = TimeUtil.toLocalDT("2023-12-05T17:00:00");
    long sampleCount = 10;
    long intervalMillis = 10_000;
    boolean updatesOnly = false;

    List<FloatEvent> stream =
        testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);

    // Check that they all equal and the last event is undefined since it should be in the future.
    assertEquals(stream.get(0).toString(), EventCode.UNDEFINED, stream.get(0).getCode());
  }

  /**
   * Test the equivalence of the three strategies when the sample happens within the stream
   * boundaries (all samples come from actual events).
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalence2() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-04T11:00:00");
    Instant end = TimeUtil.toLocalDT("2022-12-04T18:00:00");
    long sampleCount = 10;
    long intervalMillis = 10_000;
    boolean updatesOnly = false;

    testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);
  }

  /**
   * Test the equivalence of the three strategies when the sample happens across the end of the
   * stream boundary (i.e. samples after the end of the update events)
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalence3() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-05T15:00:00");
    Instant end = TimeUtil.toLocalDT("2022-12-05T18:00:00");
    long sampleCount = 10;
    long intervalMillis = 8_000_000; // ~1 hours or ~10 hour total span.
    boolean updatesOnly = false;

    List<FloatEvent> stream =
        testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);

    FloatEvent e = stream.get(stream.size() - 1);
    // Make sure the last value is what we expect.  It should match last Event, but have a timestamp
    // of the last
    // sample time.
    assertEquals(e.toString(), EventCode.UPDATE, e.getCode());
    assertEquals(e.toString(), 9021.0, e.getValue(), 1e-10);
    assertEquals(
        e.toString(), TimeUtil.toLocalDT("2022-12-06T11:00:00"), e.getTimestampAsInstant());
  }

  /**
   * Test the equivalence of the three strategies when the sample happens before start of data and
   * across the end of the stream boundary (i.e. samples before start of and after the end of the
   * update events). This test takes a couple of seconds since it has to stream across the entirety
   * of the busy stream for the slowest strategy.
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalence4() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-04T10:00:00");
    Instant end = TimeUtil.toLocalDT("2022-12-05T18:00:00");
    long sampleCount = 10;
    long intervalMillis = 20_000_000; // ~5 hours or ~50 hour total span.
    boolean updatesOnly = false;

    testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);
  }

  /**
   * Test the equivalence of the three strategies when the samples pass beyond 'now'
   *
   * @throws SQLException If trouble connecting to database
   * @throws IOException If trouble querying data
   */
  @Test
  public void testStrategyEquivalence5() throws SQLException, IOException {
    DataNexus nexus = new OnDemandNexus("docker");

    String pv = "channel5";
    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
    // First event at 2022-12-04 10:50:56
    // Last event at 2022-12-05 16:48:47
    Instant begin = TimeUtil.toLocalDT("2022-12-05T16:00:00");
    Instant end = Instant.now().plusMillis(1_000_000); // now + ~20 minutes
    long sampleCount = 10;
    // Make sure the last sample lands on the last after "now"
    long intervalMillis = (end.toEpochMilli() - begin.toEpochMilli()) / (sampleCount - 1);
    boolean updatesOnly = false;

    List<FloatEvent> stream =
        testEquivalency(begin, end, intervalMillis, sampleCount, updatesOnly, nexus, metadata);

    // Check that they all equal and the last event is undefined since it should be in the future.
    assertEquals(
        stream.get(stream.size() - 1).toString(),
        EventCode.UNDEFINED,
        stream.get(stream.size() - 1).getCode());
  }

  public <T extends Event> List<T> testEquivalency(
      Instant begin,
      Instant end,
      long intervalMillis,
      long sampleCount,
      boolean updatesOnly,
      DataNexus nexus,
      Metadata<T> metadata)
      throws SQLException, IOException {
    List<T> streamed =
        getStreamedSamples(
            begin,
            end,
            intervalMillis,
            sampleCount,
            updatesOnly,
            metadata.getType(),
            nexus,
            metadata);

    List<T> queried =
        getQueriedSamples(
            begin, intervalMillis, sampleCount, updatesOnly, metadata.getType(), nexus, metadata);

    assertFloatEventListsEqual(streamed, queried);

    return streamed;
  }

  public <T extends Event> void assertFloatEventListsEqual(List<T> l1, List<T> l2) {
    if (l1 == null && l2 == null) {
      return;
    }
    assertTrue(l1 != null && l2 != null);
    assertEquals("Unequal number of events", l1.size(), l2.size());
    for (int i = 0; i < l1.size(); i++) {
      assertEquals(
          "Timestamp: " + l1.get(i).toString() + " != " + l2.get(i).toString(),
          l1.get(i).getTimestamp(),
          l2.get(i).getTimestamp());
      assertEquals(
          "Code: " + l1.get(i).toString() + " != " + l2.get(i).toString(),
          l1.get(i).getCode(),
          l2.get(i).getCode());
      assertEquals(
          "Value: " + l1.get(i).toString() + " != " + l2.get(i).toString(),
          ((FloatEvent) l1.get(i)).getValue(),
          ((FloatEvent) l2.get(i)).getValue(),
          1e-10);
    }
  }
}
