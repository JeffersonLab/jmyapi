package org.jlab.mya.stream;

import static org.junit.Assert.*;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.junit.Test;

public class MySamplerStreamTest {

  /**
   * A method for comparing lists of FloatEvents for equality. FloatEvent does not have an equals()
   * method, so this is about the best to be done here.
   *
   * @param exp The expected list
   * @param result The actual list
   */
  public static void assertFloatEventListEquals(List<FloatEvent> exp, List<FloatEvent> result) {
    // Event does not define equals, so this is about as good as it gets.
    assertEquals("Received a different number of events than expected", exp.size(), result.size());
    for (int i = 0; i < exp.size(); i++) {
      assertEquals(
          "Value mismatch at event " + i, exp.get(i).getValue(), result.get(i).getValue(), 0.001);
      assertEquals(
          "Timestamp mismatch at event " + i,
          exp.get(i).getTimestamp(),
          result.get(i).getTimestamp());
    }
  }

  /**
   * A method for comparing lists of IntEvents for equality. IntEvent does not have an equals()
   * method, so this is about the best to be done here.
   *
   * @param exp The expected list
   * @param result The actual list
   */
  public static void assertIntEventListEquals(List<IntEvent> exp, List<IntEvent> result) {
    // Event does not define equals, so this is about as good as it gets.
    assertEquals("Received a different number of events than expected", exp.size(), result.size());
    for (int i = 0; i < exp.size(); i++) {
      assertEquals("Value mismatch at event " + i, exp.get(i).getValue(), result.get(i).getValue());
      assertEquals(
          "Timestamp mismatch at event " + i,
          exp.get(i).getTimestamp(),
          result.get(i).getTimestamp());
    }
  }

  /**
   * This test checks that the basic sampling functionality works.
   *
   * @throws IOException On error reading from underlying stream
   */
  @Test
  public void basicUsageTest() throws IOException {
    // Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20");
    // Instant end = TimeUtil.toLocalDT("2019-08-12T00:01:00.");
    // With five samples of 10s we should get samples at 00:20, 00:30, 00:40, 00:50, 01:00
    //  2019-08-12 00:00:17.289774 94.7978 -> 2019-08-12 00:00:20 94.7978
    //  2019-08-12 00:00:27.296512 95.6075 -> 2019-08-12 00:00:30 95.6075
    //  2019-08-12 00:00:38.329526 95.1736 -> 2019-08-12 00:00:40 95.1736
    //  2019-08-12 00:00:47.347886 95.2913 -> 2019-08-12 00:00:50 95.2913
    //  2019-08-12 00:00:59.311013 95.7036 -> 2019-08-12 00:00:60 95.7036

    // The stream to sample from
    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:17.28"), EventCode.UPDATE, 94.7978f);
    List<FloatEvent> events = new ArrayList<>();
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.32"), EventCode.UPDATE, 95.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.42"), EventCode.UPDATE, 0.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.52"), EventCode.UPDATE, 1.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.62"), EventCode.UPDATE, 2.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:27.27"), EventCode.UPDATE, 95.6075f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:32.27"), EventCode.UPDATE, 95.3278f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:38.32"), EventCode.UPDATE, 95.1736f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:40.33"), EventCode.UPDATE, 95.0677f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:47.34"), EventCode.UNDEFINED, 0f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:51.29"), EventCode.UPDATE, 95.4232f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:59.31"), EventCode.UPDATE, 95.7036f));

    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20.0");
    long stepMilliseconds = 10_000; // ten second sample interval
    long sampleCount = 5;
    boolean updatesOnly = false;

    // Generate the expected list
    List<FloatEvent> exp = new ArrayList<>();
    exp.add(new FloatEvent(TimeUtil.toMyaTimestamp(begin), EventCode.UPDATE, 94.7978f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(stepMilliseconds)),
            EventCode.UPDATE,
            95.6075f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(2 * stepMilliseconds)),
            EventCode.UPDATE,
            95.1736f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(3 * stepMilliseconds)),
            EventCode.UNDEFINED,
            0f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(4 * stepMilliseconds)),
            EventCode.UPDATE,
            95.7036f));

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            new ListStream<>(events, FloatEvent.class),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            FloatEvent.class)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /**
   * This test checks that the basic sampling functionality works.
   *
   * @throws IOException On error reading from underlying stream.
   */
  @Test
  public void basicUsageUpdatesOnlyTest() throws IOException {
    // Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20");
    // Instant end = TimeUtil.toLocalDT("2019-08-12T00:01:00.");
    // With five samples of 10s we should get samples at 00:20, 00:30, 00:40, 00:50, 01:00
    //  2019-08-12 00:00:17.289774 94.7978 -> 2019-08-12 00:00:20 94.7978
    //  2019-08-12 00:00:27.296512 95.6075 -> 2019-08-12 00:00:30 95.6075
    //  2019-08-12 00:00:38.329526 95.1736 -> 2019-08-12 00:00:40 95.1736
    //  2019-08-12 00:00:47.347886 95.2913 -> 2019-08-12 00:00:50 95.2913
    //  2019-08-12 00:00:59.311013 95.7036 -> 2019-08-12 00:00:60 95.7036

    // The stream to sample from
    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:17.28"), EventCode.UPDATE, 94.7978f);
    List<FloatEvent> events = new ArrayList<>();
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.32"), EventCode.UPDATE, 95.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.42"), EventCode.UPDATE, 0.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.52"), EventCode.UPDATE, 1.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.62"), EventCode.UPDATE, 2.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:27.27"), EventCode.UPDATE, 95.6075f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:32.27"), EventCode.UPDATE, 95.3278f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:38.32"), EventCode.UPDATE, 95.1736f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:40.33"), EventCode.UPDATE, 95.0677f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:51.29"), EventCode.UPDATE, 95.4232f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:59.31"), EventCode.UPDATE, 95.7036f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:59.54"), EventCode.UNDEFINED, 0f));

    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20.0");
    long stepMilliseconds = 10_000; // ten second sample interval
    long sampleCount = 5;
    boolean updatesOnly = true;

    // Generate the expected list
    List<FloatEvent> exp = new ArrayList<>();
    exp.add(new FloatEvent(TimeUtil.toMyaTimestamp(begin), EventCode.UPDATE, 94.7978f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(stepMilliseconds)),
            EventCode.UPDATE,
            95.6075f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(2 * stepMilliseconds)),
            EventCode.UPDATE,
            95.1736f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(3 * stepMilliseconds)),
            EventCode.UPDATE,
            95.0677f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(4 * stepMilliseconds)),
            EventCode.UPDATE,
            95.7036f));

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            new ListStream<>(events, FloatEvent.class),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            FloatEvent.class)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /**
   * Test the sampling behavior when getting more samples than updates.
   *
   * @throws IOException On error reading from underlying stream
   */
  @Test
  public void overSampleTest() throws IOException {
    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:19.0"), EventCode.UPDATE, 4.4f);
    List<FloatEvent> events = new ArrayList<>();
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:20.00"), EventCode.UPDATE, 5.5f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:21.00"), EventCode.UPDATE, 6.6f));

    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:19.9");
    long stepMilliseconds = 200; // ten second sample interval
    long sampleCount = 7;
    boolean updatesOnly = true;

    // Generate the expected list
    List<FloatEvent> exp = new ArrayList<>();
    exp.add(new FloatEvent(TimeUtil.toMyaTimestamp(begin), EventCode.UPDATE, 4.4f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(stepMilliseconds)), EventCode.UPDATE, 5.5f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(2 * stepMilliseconds)),
            EventCode.UPDATE,
            5.5f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(3 * stepMilliseconds)),
            EventCode.UPDATE,
            5.5f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(4 * stepMilliseconds)),
            EventCode.UPDATE,
            5.5f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(5 * stepMilliseconds)),
            EventCode.UPDATE,
            5.5f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(6 * stepMilliseconds)),
            EventCode.UPDATE,
            6.6f));

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            new ListStream<>(events, FloatEvent.class),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            FloatEvent.class)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /**
   * This check tests the behavior when we try to sample from the future. We should return the last
   * known value in order to match the behavior of the command line mySampler app. To make this test
   * repeatable, we sample from a known timestamp with data, e.g. 2019, a timestamp from tomorrow,
   * and once more from a timestamp is (tomorrow - 2019) time units past tomorrow.
   *
   * @throws IOException On error reading from underlying stream
   */
  @Test
  public void futureSampleTest() throws IOException {
    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:17.28"), EventCode.UPDATE, 94.7978f);
    List<FloatEvent> events = new ArrayList<>();
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.32"), EventCode.UPDATE, 95.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.42"), EventCode.UPDATE, 0.0715f));
    events.add(
        new FloatEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.52"), EventCode.UPDATE, 1.0715f));

    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20.0");
    Instant tomorrow = Instant.now().plusMillis(1000 * 60 * 60 * 24);
    long stepMilliseconds = begin.until(tomorrow, ChronoUnit.MILLIS) / 2;
    long sampleCount = 3;
    boolean updatesOnly = false;

    List<FloatEvent> exp = new ArrayList<>();
    exp.add(new FloatEvent(TimeUtil.toMyaTimestamp(begin), EventCode.UPDATE, 94.7978f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(stepMilliseconds)),
            EventCode.UPDATE,
            1.0715f));
    exp.add(
        new FloatEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(2 * stepMilliseconds)),
            EventCode.UNDEFINED,
            0f));

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            new ListStream<>(events, FloatEvent.class),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            FloatEvent.class)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /** Test that the MySamplerStream constructor returns all UNDEFINED events if no data is given. */
  @Test
  public void nullPriorPointTest1() throws IOException {
    System.out.println("priorPoint == " + null);
    ListStream<FloatEvent> empty = new ListStream<>(new ArrayList<>(), FloatEvent.class);
    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:00.00");
    long intervalMillis = 1000;

    List<FloatEvent> exp = new ArrayList<>();
    FloatEvent undef = new FloatEvent(begin, EventCode.UNDEFINED, 0);
    exp.add(undef);
    exp.add(undef.copyTo(begin.plusMillis(intervalMillis)));
    exp.add(undef.copyTo(begin.plusMillis(intervalMillis * 2)));

    List<FloatEvent> result = new ArrayList<>();
    FloatEvent event;
    try (MySamplerStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            empty, begin, intervalMillis, 3, null, false, FloatEvent.class)) {
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /** Test that the MySamplerStream constructor returns UNDEFINED events until data is available. */
  @Test
  public void nullPriorPointTest2() throws IOException {
    System.out.println("priorPoint == " + null);

    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:00.00");
    long intervalMillis = 1000;
    long sampleCount = 5;

    List<FloatEvent> events = new ArrayList<>();
    FloatEvent e1 = new FloatEvent(begin.plusMillis(1001), EventCode.UPDATE, 10);
    FloatEvent e2 = new FloatEvent(begin.plusMillis(2001), EventCode.UPDATE, 20);
    FloatEvent e3 = new FloatEvent(begin.plusMillis(3001), EventCode.UPDATE, 30);
    events.add(e1);
    events.add(e2);
    events.add(e3);
    ListStream<FloatEvent> wrapped = new ListStream<>(events, FloatEvent.class);

    List<FloatEvent> exp = new ArrayList<>();

    FloatEvent undef = new FloatEvent(begin, EventCode.UNDEFINED, 0);
    exp.add(undef);
    exp.add(undef.copyTo(begin.plusMillis(intervalMillis)));
    exp.add(e1.copyTo(begin.plusMillis(intervalMillis * 2)));
    exp.add(e2.copyTo(begin.plusMillis(intervalMillis * 3)));
    exp.add(e3.copyTo(begin.plusMillis(intervalMillis * 4)));

    List<FloatEvent> result = new ArrayList<>();
    FloatEvent event;
    try (MySamplerStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            wrapped, begin, intervalMillis, sampleCount, null, false, FloatEvent.class)) {
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /**
   * Check that the sampling works OK with only a single point in the stream. Same value should be
   * repeated at the requested sample times
   *
   * @throws IOException On error reading from underlying stream
   */
  @Test
  public void singlePointTest() throws IOException {
    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20.0");
    long stepMilliseconds = 1000;
    long sampleCount = 4;
    boolean updatesOnly = false;

    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toMyaTimestamp(begin.minusMillis(1000)), EventCode.UPDATE, 10);
    System.out.println("Single prior point: " + priorPoint);
    List<FloatEvent> exp = new ArrayList<>();
    exp.add(priorPoint.copyTo(begin));
    exp.add(priorPoint.copyTo(begin.plusMillis(stepMilliseconds)));
    exp.add(priorPoint.copyTo(begin.plusMillis(2 * stepMilliseconds)));
    exp.add(priorPoint.copyTo(begin.plusMillis(3 * stepMilliseconds)));

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        MySamplerStream.getMySamplerStream(
            new ListStream<>(new ArrayList<>(), FloatEvent.class),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            FloatEvent.class)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertFloatEventListEquals(exp, result);
  }

  /**
   * Test that this stream works with a different Event type than just FloatEvent. This should be
   * enough to make sure no FloatEvent specific code has crept in.
   *
   * @throws IOException On error reading from underlying stream
   */
  @Test
  public void intEventTest() throws IOException {
    // The stream to sample from
    IntEvent priorPoint =
        new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:17.28"), EventCode.UPDATE, 94);
    List<IntEvent> events = new ArrayList<>();
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.32"), EventCode.UPDATE, 95));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.42"), EventCode.UPDATE, 0));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.52"), EventCode.UPDATE, 1));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:23.62"), EventCode.UPDATE, 2));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:27.27"), EventCode.UPDATE, 95));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:32.27"), EventCode.UPDATE, 96));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:38.32"), EventCode.UPDATE, 90));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:40.33"), EventCode.UPDATE, 91));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:47.34"), EventCode.UPDATE, 92));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:51.29"), EventCode.UPDATE, 93));
    events.add(new IntEvent(TimeUtil.toLocalDT("2019-08-12T00:00:59.31"), EventCode.UPDATE, 94));

    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:20.0");
    long stepMilliseconds = 10_000; // ten second sample interval
    long sampleCount = 5;
    boolean updatesOnly = false;

    // Generate the expected list
    List<IntEvent> exp = new ArrayList<>();
    exp.add(new IntEvent(TimeUtil.toMyaTimestamp(begin), EventCode.UPDATE, 94));
    exp.add(
        new IntEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(stepMilliseconds)), EventCode.UPDATE, 95));
    exp.add(
        new IntEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(2 * stepMilliseconds)), EventCode.UPDATE, 90));
    exp.add(
        new IntEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(3 * stepMilliseconds)), EventCode.UPDATE, 92));
    exp.add(
        new IntEvent(
            TimeUtil.toMyaTimestamp(begin.plusMillis(4 * stepMilliseconds)), EventCode.UPDATE, 94));

    List<IntEvent> result = new ArrayList<>();
    try (EventStream<IntEvent> stream =
        MySamplerStream.getMySamplerStream(
            new ListStream<>(events, IntEvent.class),
            begin,
            stepMilliseconds,
            sampleCount,
            priorPoint,
            updatesOnly,
            IntEvent.class)) {
      IntEvent event;
      while ((event = stream.read()) != null) {
        result.add(event);
        System.out.println(event);
      }
    }

    assertIntEventListEquals(exp, result);
  }
}
