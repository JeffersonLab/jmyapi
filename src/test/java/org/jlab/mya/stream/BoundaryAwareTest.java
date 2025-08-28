package org.jlab.mya.stream;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.junit.Test;

public class BoundaryAwareTest {

  /**
   * Test boundary aware stream with prior point provided and bounds are totally within the past.
   */
  @Test
  public void testAddBoth() throws Exception {
    Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2019-06-01T00:00:00");

    List<FloatEvent> events = new ArrayList<>();
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-02-01T00:00:00"), EventCode.UPDATE, 1));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-03-01T00:00:00"), EventCode.UPDATE, 2));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-04-01T00:00:00"), EventCode.UPDATE, 3));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-05-01T00:00:00"), EventCode.UPDATE, 4));

    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2018-12-01T00:00:00"), EventCode.UPDATE, 0);

    long count = events.size();

    System.out.println("count: " + count);

    long expSize = 6;

    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (BoundaryAwareStream<FloatEvent> boundaryStream =
          new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class)) {
        FloatEvent event;
        while ((event = boundaryStream.read()) != null) {
          eventList.add(event);
          System.out.println(event.toString());
        }
      }
    }

    assertEquals("List size does not match expected", expSize, eventList.size());
  }

  /** Test null prior point. */
  @Test
  public void testAddEndOnly() throws Exception {
    Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2019-06-01T00:00:00");

    List<FloatEvent> events = new ArrayList<>();
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-02-01T00:00:00"), EventCode.UPDATE, 1));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-03-01T00:00:00"), EventCode.UPDATE, 2));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-04-01T00:00:00"), EventCode.UPDATE, 3));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-05-01T00:00:00"), EventCode.UPDATE, 4));

    FloatEvent priorPoint = null;

    long count = events.size();

    System.out.println("count: " + count);

    long expSize = 5;

    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (BoundaryAwareStream<FloatEvent> boundaryStream =
          new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class)) {
        FloatEvent event;
        while ((event = boundaryStream.read()) != null) {
          eventList.add(event);
          System.out.println(event.toString());
        }
      }
    }

    assertEquals("List size does not match expected", expSize, eventList.size());
  }

  /** Test all non-update events. */
  @Test
  public void testAddNeither() throws Exception {
    Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2019-06-01T00:00:00");

    List<FloatEvent> events = new ArrayList<>(); /* All points are non-update events! */
    events.add(
        new FloatEvent(
            TimeUtil.toLocalDT("2019-02-01T00:00:00"), EventCode.NETWORK_DISCONNECTION, 1));
    events.add(
        new FloatEvent(
            TimeUtil.toLocalDT("2019-03-01T00:00:00"), EventCode.NETWORK_DISCONNECTION, 2));
    events.add(
        new FloatEvent(
            TimeUtil.toLocalDT("2019-04-01T00:00:00"), EventCode.NETWORK_DISCONNECTION, 3));
    events.add(
        new FloatEvent(
            TimeUtil.toLocalDT("2019-05-01T00:00:00"), EventCode.NETWORK_DISCONNECTION, 4));

    FloatEvent priorPoint = null;

    long count = events.size();

    System.out.println("count: " + count);

    long expSize = 4;

    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (BoundaryAwareStream<FloatEvent> boundaryStream =
          new BoundaryAwareStream<>(stream, begin, end, priorPoint, true, FloatEvent.class)) {
        FloatEvent event;
        while ((event = boundaryStream.read()) != null) {
          eventList.add(event);
          System.out.println(event.toString());
        }
      }
    }

    assertEquals("List size does not match expected", expSize, eventList.size());
  }

  /** Test boundary end date 200 years in future! We are looking for last point to be "now". */
  @Test
  public void testNowAdjustedEnd() throws Exception {
    Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2219-06-01T00:00:00"); // 200 years into future!

    List<FloatEvent> events = new ArrayList<>();
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-02-01T00:00:00"), EventCode.UPDATE, 1));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-03-01T00:00:00"), EventCode.UPDATE, 2));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-04-01T00:00:00"), EventCode.UPDATE, 3));
    events.add(new FloatEvent(TimeUtil.toLocalDT("2019-05-01T00:00:00"), EventCode.UPDATE, 4));

    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2018-12-01T00:00:00"), EventCode.UPDATE, 0);

    long count = events.size();

    System.out.println("count: " + count);

    long expSize = 6;

    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (BoundaryAwareStream<FloatEvent> boundaryStream =
          new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class)) {
        FloatEvent event;
        while ((event = boundaryStream.read()) != null) {
          eventList.add(event);
          System.out.println(event.toString());
        }
      }
    }

    assertEquals("List size does not match expected", expSize, eventList.size());
  }

  /** Test boundary aware stream with prior point but otherwise an empty stream! */
  @Test
  public void testPriorWithEmptyStream() throws Exception {
    Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2019-06-01T00:00:00");

    List<FloatEvent> events = new ArrayList<>();

    FloatEvent priorPoint =
        new FloatEvent(TimeUtil.toLocalDT("2018-12-01T00:00:00"), EventCode.UPDATE, 0);

    long count = events.size();

    System.out.println("count: " + count);

    long expSize = 2;

    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (BoundaryAwareStream<FloatEvent> boundaryStream =
          new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class)) {
        FloatEvent event;
        while ((event = boundaryStream.read()) != null) {
          eventList.add(event);
          System.out.println(event.toString());
        }
      }
    }

    assertEquals("List size does not match expected", expSize, eventList.size());
  }

  /**
   * Test boundary aware stream with no prior point and an empty stream! Since no "last point" we
   * can't have end boundary either!
   */
  @Test
  public void testEmptyStream() throws Exception {
    Instant begin = TimeUtil.toLocalDT("2019-01-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2019-06-01T00:00:00");

    List<FloatEvent> events = new ArrayList<>();

    FloatEvent priorPoint = null;

    long count = events.size();

    System.out.println("count: " + count);

    long expSize = 0;

    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (BoundaryAwareStream<FloatEvent> boundaryStream =
          new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class)) {
        FloatEvent event;
        while ((event = boundaryStream.read()) != null) {
          eventList.add(event);
          System.out.println(event.toString());
        }
      }
    }

    assertEquals("List size does not match expected", expSize, eventList.size());
  }
}
